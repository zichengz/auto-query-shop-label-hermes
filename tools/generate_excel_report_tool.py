import os
import subprocess
import json
from typing import Dict, Any

from tools.registry import registry, tool_error

def generate_excel_report(predict_jsonl: str, file_path: str, sample_path: str, use_date: str, user_name: str, explain_num: int = 1000, mock: bool = False) -> str:
    if not os.path.exists(predict_jsonl) and not mock:
        return tool_error(f"Prediction JSONL not found: {predict_jsonl}")
        
    dir_path = os.path.dirname(file_path)
    filename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    
    output_path_current = os.path.join(dir_path, "output", f"{use_date}_{filename_without_ext}")
    result_path = os.path.join(output_path_current, "result.txt")
    
    # original path: /nfs/dataset-ofs-ssl/jiangyaogang/autollm/groceryQueryShopllm/files/result_details/${use_date}_${user_name}_${filename_without_ext}_result_details.xlsx
    # For hermes execution, we can write locally to dir_path
    result_details_path = os.path.join(dir_path, f"{use_date}_{user_name}_{filename_without_ext}_result_details.xlsx")
    
    if mock:
        os.makedirs(output_path_current, exist_ok=True)
        with open(result_path, 'w') as f:
            f.write("mocked result\n")
        with open(result_details_path, 'w') as f:
            f.write("mocked excel\n")
        return json.dumps({
            "status": "success",
            "message": f"MOCKED Report generated: {result_details_path}",
            "result_details_path": result_details_path
        }, ensure_ascii=False)
        
    current_dir = os.path.dirname(os.path.abspath(__file__))
    legacy_script_dir = os.path.abspath(os.path.join(current_dir, "../apps/grocery_label/legacy_scripts"))
    process_script = os.path.join(legacy_script_dir, "process_result.py")
    details_script = os.path.join(legacy_script_dir, "result_details.py")
    
    try:
        res1 = subprocess.run(
            ["python", "-u", process_script, predict_jsonl, file_path, result_path, str(explain_num)],
            check=True,
            capture_output=True,
            text=True
        )
        res2 = subprocess.run(
            ["python", "-u", details_script, sample_path, predict_jsonl, result_details_path, file_path, str(explain_num)],
            check=True,
            capture_output=True,
            text=True
        )
        return json.dumps({
            "status": "success",
            "message": f"Report generated successfully: {result_details_path}",
            "result_details_path": result_details_path
        }, ensure_ascii=False)
    except subprocess.CalledProcessError as e:
        error_msg = f"Report generation script failed (exit code {e.returncode}):\n"
        if e.stderr:
            error_msg += f"STDERR:\n{e.stderr}\n"
        if e.stdout:
            stdout_str = e.stdout if len(e.stdout) < 2000 else "...[TRUNCATED]...\n" + e.stdout[-2000:]
            error_msg += f"STDOUT (tail):\n{stdout_str}\n"
        return tool_error(error_msg)
    except Exception as e:
        return tool_error(f"Unexpected error: {str(e)}")

def check_requirements() -> bool:
    return True

SCHEMA = {
    "name": "generate_excel_report",
    "description": "Generates the final Excel report after LLM inference is complete.",
    "parameters": {
        "type": "object",
        "properties": {
            "predict_jsonl": {
                "type": "string",
                "description": "Path to the merged LLM outputs (JSONL)"
            },
            "file_path": {
                "type": "string",
                "description": "Original input file path"
            },
            "sample_path": {
                "type": "string",
                "description": "Path to the original feature dataset (before augmentation)"
            },
            "use_date": {
                "type": "string",
                "description": "Date string"
            },
            "user_name": {
                "type": "string",
                "description": "User name"
            },
            "explain_num": {
                "type": "integer",
                "description": "Number of explanations",
                "default": 1000
            },
            "mock": {
                "type": "boolean",
                "description": "If true, mocks the generation process."
            }
        },
        "required": ["predict_jsonl", "file_path", "sample_path", "use_date", "user_name"]
    }
}

registry.register(
    name="generate_excel_report",
    toolset="grocery",
    schema=SCHEMA,
    handler=lambda args, **kwargs: generate_excel_report(
        predict_jsonl=args["predict_jsonl"],
        file_path=args["file_path"],
        sample_path=args["sample_path"],
        use_date=args["use_date"],
        user_name=args["user_name"],
        explain_num=args.get("explain_num", 1000),
        mock=args.get("mock", False)
    ),
    check_fn=check_requirements,
    emoji="📊"
)
