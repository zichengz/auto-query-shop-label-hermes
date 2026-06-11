import os
import subprocess
import json
from typing import Dict, Any

from tools.registry import registry, tool_error

def run_spark_feature(user_name: str, file_path: str, use_date: str, mock: bool = False) -> str:
    """
    Submits the PySpark job to extract Hive/HDFS features.
    
    Args:
        user_name: Username (e.g., guoshubin)
        file_path: Local input file path
        use_date: Date string (e.g., 20251112)
        mock: If True, skips actual cluster execution and mocks the output
    """
    if not os.path.exists(file_path):
        return tool_error(f"Input file does not exist: {file_path}")
        
    dir_path = os.path.dirname(file_path)
    filename_with_ext = os.path.basename(file_path)
    filename_without_ext = os.path.splitext(filename_with_ext)[0]
    
    output_path_current = os.path.join(dir_path, 'output', f"{use_date}_{filename_without_ext}")
    sample_path = os.path.join(output_path_current, 'query_shop_info')
    
    if not os.path.exists(output_path_current):
        os.makedirs(output_path_current)
        
    if mock:
        # Mock logic: Create a dummy output file for testing local Multi-Agent coordination
        with open(sample_path, 'w') as f:
            # Writing a dummy line matching the expected 15 columns format:
            # index, sid, query, normalquery, query_tag, query_intent, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info
            dummy_line = "0\ts123\tmilk\tmilk\ttag\titem\tShopA\tshopa\tcat\tcat2\t\t\t\titem_milk\ttag"
            f.write(dummy_line + "\n")
            
            # Write a second dummy line
            dummy_line2 = "1\ts124\tbread\tbread\ttag\titem\tShopB\tshopb\tcat\tcat2\t\t\t\titem_bread\ttag"
            f.write(dummy_line2 + "\n")
            
        return json.dumps({
            "status": "success",
            "message": f"MOCKED Spark job completed. Output saved to {sample_path}",
            "sample_path": sample_path
        }, ensure_ascii=False)
        
    # Real logic
    source_sample_hdfs = f"/user/prod_soda_trade_strategy/nlp/llmAutoLabel/llm_mx_grocery/data_manual/source/{user_name}"
    source_sample_hdfs_file = f"{source_sample_hdfs}/{filename_with_ext}"
    sample_hdfs = f"/user/prod_soda_trade_strategy/nlp/llmAutoLabel/llm_mx_grocery/data_manual/output/{user_name}/{filename_without_ext}"
    
    try:
        # 1. Ensure HDFS directory
        subprocess.run(f"hadoop fs -mkdir -p {source_sample_hdfs}", shell=True, check=False)
        
        # 2. Upload file
        subprocess.run(f"hadoop fs -rm -f {source_sample_hdfs_file}", shell=True, check=False)
        subprocess.run(f"hadoop fs -put {file_path} {source_sample_hdfs}", shell=True, check=True)
        
        # 3. Submit Spark Job
        spark_cmd = (
            f"spark-submit "
            f"--deploy-mode client "
            f"--queue root.soda_i18n_trading_stg_prod "
            f"--executor-cores 2 "
            f"--driver-memory 8G "
            f"--executor-memory 8G "
            f"--conf spark.executor.memoryOverhead=7G "
            f"--conf spark.default.parallelism=100 "
            f"--conf spark.dynamicAllocation.minExecutors=32 "
            f"--conf spark.dynamicAllocation.maxExecutors=50 "
            f"get_manual.py {source_sample_hdfs_file} {sample_hdfs} {use_date}"
        )
        
        # Ensure we run from legacy_scripts directory so spark-submit can find get_manual.py
        current_dir = os.path.dirname(os.path.abspath(__file__))
        legacy_script_dir = os.path.abspath(os.path.join(current_dir, "../apps/grocery_label/legacy_scripts"))
        subprocess.run(spark_cmd, shell=True, check=True, cwd=legacy_script_dir)
        
        # 4. Download Result
        subprocess.run(f"hadoop fs -cat {sample_hdfs}/* > {sample_path}", shell=True, check=True)
        
        return json.dumps({
            "status": "success",
            "message": f"Spark job completed. Output downloaded to {sample_path}",
            "sample_path": sample_path
        }, ensure_ascii=False)
        
    except subprocess.CalledProcessError as e:
        return tool_error(f"Subprocess failed with error: {str(e)}")
    except Exception as e:
        return tool_error(f"Unexpected error: {str(e)}")

def check_requirements() -> bool:
    return True

SCHEMA = {
    "name": "run_spark_feature",
    "description": "Submits a PySpark job to extract Hive/HDFS features for grocery labeling and downloads the result to a local path.",
    "parameters": {
        "type": "object",
        "properties": {
            "user_name": {
                "type": "string",
                "description": "Username (e.g., guoshubin)"
            },
            "file_path": {
                "type": "string",
                "description": "Absolute path to the input text file"
            },
            "use_date": {
                "type": "string",
                "description": "Date string in YYYYMMDD format"
            },
            "mock": {
                "type": "boolean",
                "description": "If true, bypasses the cluster commands and returns a mocked success file. Useful for local testing."
            }
        },
        "required": ["user_name", "file_path", "use_date"]
    }
}

registry.register(
    name="run_spark_feature",
    toolset="grocery",
    schema=SCHEMA,
    handler=lambda args, **kwargs: run_spark_feature(
        user_name=args["user_name"],
        file_path=args["file_path"],
        use_date=args["use_date"],
        mock=args.get("mock", False)
    ),
    check_fn=check_requirements,
    emoji="⚡"
)
