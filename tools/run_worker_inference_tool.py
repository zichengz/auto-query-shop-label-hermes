import os
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

from tools.registry import registry, tool_error

# Add legacy scripts to path to import templates
current_dir = os.path.dirname(os.path.abspath(__file__))
legacy_scripts_path = os.path.abspath(os.path.join(current_dir, "../apps/grocery_label/legacy_scripts"))
if legacy_scripts_path not in sys.path:
    sys.path.append(legacy_scripts_path)

try:
    from prompt_template import prompt_templates
except ImportError as e:
    # Fallback to absolute if somehow the path resolution didn't work in different contexts
    print(f"Warning: Failed to import prompt_templates natively: {e}")
    prompt_templates = {}

def format_prompt(info: list, prompt_template_v: str, prompt_template_single: str, explain_num: int):
    index, ori_query, query, query_tag, query_intent, shop_id, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_list, item_tag_list, shop_info = info
    index = int(index)
    
    if index < explain_num:
        version = prompt_template_v
    else:
        version = prompt_template_single
        
    prompt_template = prompt_templates.get(version, "Missing Template: {query}")
    
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
        
    format_args = {
        "query": ori_query,
        "may_shop_info": shop_info_str,
        "query_tag": query_tag,
        "query_intent": query_intent,
        "shop_name": shop_name,
        "first_tag_name": category_l3_tag_local,
        "second_tag_name": category_l3_tag_local_idx2, 
        "item_name_list": item_name_list,
        "item_tag_list": item_tag_list,
    }
    
    prompt = prompt_template.format(**format_args)
    return prompt, index, ori_query, query, shop_id, query_intent, shop_name, version

def process_chunk_with_worker(chunk_idx: int, lines: list, agent_config: dict, explain_num: int, output_dir: str) -> str:
    from run_agent import AIAgent
    worker = AIAgent(ephemeral_system_prompt="You are a strict data labeling assistant.", **agent_config)
    output_path = os.path.join(output_dir, f"output_chunk_{chunk_idx}.jsonl")
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
            # Do not silently drop rows on API failure!
            valid = f'api error: {str(e)}'
            result = {
                "ori_query": info.get('query', ''),
                "query_name": info.get('normalquery', ''),
                "shop_name": info.get('shop_name', ''),
                "sp_key": info.get('query', '') + "-" + info.get('sid', ''),
                "intent": info.get('query_intent', ''),
                "valid": valid,
                "prompt": "",
                "prediction": "error",
                "index": int(info.get('index', -1)),
                "prompt_ver": "error",
                "shop_id": info.get('sid', '')
            }
            results.append(result)
    with open(output_path, 'w', encoding='utf-8') as f:
        for res in results:
            f.write(json.dumps(res, ensure_ascii=False) + '\n')
            
    return output_path

def run_worker_inference(sample_path_new: str, explain_num: int = 1000, num_workers: int = 2, mock: bool = False, model_name: str = "qwen-plus") -> str:
    if not os.path.exists(sample_path_new):
        return tool_error(f"Augmented sample file not found: {sample_path_new}")
        
    output_dir = os.path.dirname(sample_path_new)
    merged_jsonl_path = os.path.join(output_dir, "merged_output.jsonl")
    
    if mock:
        # Just create dummy output
        with open(merged_jsonl_path, 'w', encoding='utf-8') as f:
            f.write('{"ori_query": "mock", "prediction": "mocked"}\n')
        return json.dumps({
            "status": "success",
            "message": f"MOCKED Worker inference completed. Output saved to {merged_jsonl_path}",
            "merged_jsonl_path": merged_jsonl_path
        }, ensure_ascii=False)
        
    with open(sample_path_new, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    chunk_size = max(1, (len(lines) + num_workers - 1) // num_workers)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size) if lines[i:i + chunk_size]]
    
    if "gemini" in model_name.lower():
        base_url = "http://10.15.58.57:4000/v1" if "v1" not in "http://10.15.58.57:4000" else "http://10.15.58.57:4000"
        api_key = "sk-4fnsm8yujqlO5AbmOPZuqw"
    else:
        base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
        api_key = "sk-e4aaa558423246ad8095259814a285ae"

    agent_config = {
        "model": model_name,
        "base_url": base_url,
        "api_key": api_key,
        "quiet_mode": True
    }
    
    chunk_files = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(process_chunk_with_worker, i, chunk, agent_config, explain_num, output_dir))
            
        for future in as_completed(futures):
            chunk_files.append(future.result())
            
    with open(merged_jsonl_path, 'w', encoding='utf-8') as outfile:
        for chunk_file in chunk_files:
            if os.path.exists(chunk_file):
                with open(chunk_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
                os.remove(chunk_file) # clean up
                    
    return json.dumps({
        "status": "success",
        "message": f"Worker inference completed. Output merged to {merged_jsonl_path}",
        "merged_jsonl_path": merged_jsonl_path
    }, ensure_ascii=False)

def check_requirements() -> bool:
    return True

SCHEMA = {
    "name": "run_worker_inference",
    "description": "Orchestrates Hermes AIAgent workers to run LLM inference on the dataset and produce predictions.",
    "parameters": {
        "type": "object",
        "properties": {
            "sample_path_new": {
                "type": "string",
                "description": "Path to the augmented feature dataset (query_shop_info_new)"
            },
            "explain_num": {
                "type": "integer",
                "description": "Number of explanations to output (default 1000)"
            },
            "num_workers": {
                "type": "integer",
                "description": "Number of parallel worker threads to spawn (default 2)"
            },
            "mock": {
                "type": "boolean",
                "description": "If true, mocks the worker inference process."
            },
            "model_name": {
                "type": "string",
                "description": "Model to use for inference, e.g., 'qwen-plus' or 'gemini-2.5-pro'"
            }
        },
        "required": ["sample_path_new"]
    }
}

registry.register(
    name="run_worker_inference",
    toolset="grocery",
    schema=SCHEMA,
    handler=lambda args, **kwargs: run_worker_inference(
        sample_path_new=args["sample_path_new"],
        explain_num=args.get("explain_num", 1000),
        num_workers=args.get("num_workers", 2),
        mock=args.get("mock", False),
        model_name=args.get("model_name", "qwen-plus")
    ),
    check_fn=check_requirements,
    emoji="🧠"
)
