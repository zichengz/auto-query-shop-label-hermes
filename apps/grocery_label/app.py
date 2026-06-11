#!/usr/bin/env python3
import sys
import os
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure run_agent.py is in the python path
hermes_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(hermes_path)

# Add legacy scripts to path to import templates
grocery_path = os.path.abspath(os.path.join(hermes_path, 'apps/grocery_label/legacy_scripts'))
sys.path.append(grocery_path)

try:
    from run_agent import AIAgent
    import tools.run_spark_feature_tool
    import tools.augment_shop_knowledge_tool
    import tools.generate_excel_report_tool
except ImportError as e:
    print(f"Failed to import Hermes Agent or Tools: {e}")
    sys.exit(1)

try:
    from prompt_template import prompt_templates
except ImportError as e:
    print(f"Failed to import prompt_templates from original codebase: {e}")
    sys.exit(1)

def format_prompt(info: list, prompt_template_v: str, prompt_template_single: str, explain_num: int):
    # Matches the TSV parsing in generate_labeled_data_intent.py
    index, ori_query, query, query_tag, query_intent, shop_id, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_list, item_tag_list, shop_info = info
    index = int(index)
    
    if index < explain_num:
        version = prompt_template_v
    else:
        version = prompt_template_single
        
    prompt_template = prompt_templates[version]
    
    shop_info_str = ""
    shop_info_list = shop_info.split("!.!")
    
    for idx, info_item in enumerate(shop_info_list):
        if idx >= 3:
            continue
        items = info_item.split("~.~")
        if len(items) != 4:
            continue
        shopname, shopnum, manner_info, item_info = items
        shop_info_str += "可能与query相关的店铺"+ str(idx)+ ":"+ "店铺名("+ shopname+ ")"
        if manner_info != "":
            shop_info_str += ", 店铺主营（"+ manner_info+ ")"
        if item_info != "":
            shop_info_str += ", 店铺部分商品（"+ item_info+ ")"
        shop_info_str += "\n"
        
    if shop_info_str != "":
        shop_info_str += "上面的可能相关的店铺信息只是作为参考，辅助判断query是否是商超店铺需求\n"
        
    # Safe format to prevent missing keys in v5
    format_args = {
        "query": ori_query,
        "may_shop_info": shop_info_str,
        "query_tag": query_tag,
        "query_intent": query_intent,
        "shop_name": shop_name,
        "first_tag_name": category_l3_tag_local,
        "second_tag_name": category_l3_tag_local_idx2, # Added to fix original script's bug for v5
        "item_name_list": item_name_list,
        "item_tag_list": item_tag_list,
    }
    
    prompt = prompt_template.format(**format_args)
    return prompt, index, ori_query, query, shop_id, query_intent, shop_name, version

def process_chunk_with_worker(chunk_idx: int, lines: list, agent_config: dict, explain_num: int) -> str:
    """Worker Agent runs to process a chunk of data"""
    # The worker agent is stateless here, initialized with empty system prompt
    # because the exact prompt template already contains the persona and instructions.
    worker = AIAgent(ephemeral_system_prompt="You are a strict data labeling assistant.", **agent_config)
    
    output_path = f"output_chunk_{chunk_idx}.jsonl"
    results = []
    
    for line in lines:
        if not line.strip() or len(line.strip().split("\t")) != 16:
            continue
            
        info = line.strip().split("\t")
        try:
            prompt, index, ori_query, query, shop_id, query_intent, shop_name, version = format_prompt(
                info, "v4", "v5", explain_num
            )
            
            response = worker.run_conversation(prompt)
            response_text = response.get('final_response') if isinstance(response, dict) else str(response)
            
            pred = None
            valid = 1
            if index < explain_num:
                # Need to parse JSON
                json_str = re.sub(r'^```json|```$', '', response_text, flags=re.MULTILINE).strip()
                try:
                    pred = json.loads(json_str)
                except Exception:
                    valid = 'json load error'
                    pred = response_text
            else:
                pred = response_text
                
            result = {
                "ori_query": ori_query,
                "query_name": query,
                "shop_name": shop_name,
                "sp_key": ori_query + "-" + shop_id,
                "intent": query_intent,
                "valid": valid,
                "prompt": prompt,
                "prediction": pred,
                "index": index,
                "prompt_ver": version,
                "shop_id": shop_id
            }
            results.append(result)
            
        except Exception as e:
            print(f"Error processing line: {str(e)}")
            
    with open(output_path, 'w', encoding='utf-8') as f:
        for res in results:
            f.write(json.dumps(res, ensure_ascii=False) + '\n')
            
    return output_path

COORDINATOR_PROMPT = """You are the Coordinator Agent for the Grocery Labeling Multi-Agent System.
Your job is to orchestrate the pipeline:
1. Run the Spark feature extraction tool (`run_spark_feature`).
2. Run the shop knowledge augmentation tool (`augment_shop_knowledge`).
3. Notify the user that data is prepared, then the python script will partition the data and invoke Worker agents.
4. After worker agents complete, you will run the report generation tool (`generate_excel_report`).

VERY IMPORTANT: When calling `generate_excel_report`, you MUST pass the original `sample_path` (the one returned by `run_spark_feature`), NOT the `sample_path_new` returned by `augment_shop_knowledge`.

Keep the user updated. Follow the user's instructions regarding inputs (user_name, file_path, use_date).
"""

def get_response_text(reply):
    if isinstance(reply, dict):
        return reply.get('final_response') or ''
    return str(reply)

def main():
    print("=========================================")
    print("  Grocery Labeling Multi-Agent Pipeline  ")
    print("=========================================\n")
    
    agent_config = {
        "model": "qwen-plus",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "api_key": "sk-e4aaa558423246ad8095259814a285ae",
        "quiet_mode": True
    }
    
    coordinator_toolsets = ["grocery"]
    
    coordinator = AIAgent(
        ephemeral_system_prompt=COORDINATOR_PROMPT,
        enabled_toolsets=coordinator_toolsets,
        **agent_config
    )
    
    print("▶ Coordinator Initialization...")
    
    file_path = "/Users/didi/Documents/code/groceryQueryShopllm/test.txt"
    user_name = "guoshubin"
    use_date = "20260611"
    explain_num = 1000
    
    initial_instruction = (
        f"Please prepare the features for user '{user_name}' using file '{file_path}' "
        f"and date '{use_date}'. Use `mock=True` since we are running locally."
    )
    
    print(f"\nUser: {initial_instruction}\n")
    reply = coordinator.run_conversation(initial_instruction)
    print(f"Coordinator: {get_response_text(reply)}\n")
    
    sample_path = f"{os.path.dirname(file_path)}/output/{use_date}_test/query_shop_info"
    sample_path_new = f"{sample_path}_new"
    
    # Check if sample_path_new exists before continuing
    if not os.path.exists(sample_path_new):
        print(f"File {sample_path_new} not found. Ensure tools ran successfully.")
        # Create dummy for testing if mocked
        os.makedirs(os.path.dirname(sample_path_new), exist_ok=True)
        # Create 16-column TSV dummy
        with open(sample_path_new, "w") as f:
            dummy_line = "0\ts123\tmilk\tmilk\ttag\titem\tShopA\tshopa\tcat\tcat2\t\t\t\titem_milk\ttag\tMockShop~.~1~.~MockTag~.~MockItem"
            f.write(dummy_line + "\n")
            
    print("\n▶ Partitioning Data and Dispatching Worker Agents...")
    with open(sample_path_new, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    num_workers = 2
    chunk_size = max(1, (len(lines) + num_workers - 1) // num_workers)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size) if lines[i:i + chunk_size]]
    
    chunk_files = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(process_chunk_with_worker, i, chunk, agent_config, explain_num))
            
        for future in as_completed(futures):
            chunk_files.append(future.result())
            
    print(f"\n▶ Worker Agents completed. Produced {len(chunk_files)} chunk files.")
    
    merged_jsonl_path = os.path.join(os.path.dirname(sample_path_new), "merged_output.jsonl")
    with open(merged_jsonl_path, 'w', encoding='utf-8') as outfile:
        for chunk_file in chunk_files:
            if os.path.exists(chunk_file):
                with open(chunk_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
                    
    print(f"▶ Merged output to {merged_jsonl_path}")
    
    report_instruction = (
        f"The worker agents have completed inference and saved to '{merged_jsonl_path}'. "
        f"Please run the excel report generator tool. Remember to use sample_path='{sample_path}' "
        f"and file_path='{file_path}'. "
        f"Use mock=True."
    )
    
    print(f"\nUser: {report_instruction}\n")
    reply = coordinator.run_conversation(report_instruction)
    print(f"Coordinator: {get_response_text(reply)}\n")
    
    print("=========================================")
    print("           Pipeline Complete             ")
    print("=========================================\n")

if __name__ == "__main__":
    main()
