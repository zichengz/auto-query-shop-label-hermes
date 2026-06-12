---
name: grocery-labeling-pipeline
description: 执行外卖平台商超搜索 (Query-Shop) 相关性大模型打标流水线（Feature Extraction -> Knowledge Augmentation -> Multi-agent Inference -> Report Generation）
---

# 外卖商超搜索 Query-Shop 相关性打标 SOP

当用户要求执行“商超搜索打标”、“跑全套流水线”、“跑一下 query-shop 打标”等相关意图时，必须严格遵守以下执行规范，无需再让用户提供详细的运行逻辑。

## 1. 必需的运行参数
在开始执行前，检查用户是否提供了以下三个必需参数。如果缺失，请先询问用户：
- `user_name`: 用户名（例如：guoshubin）
- `file_path`: 输入文本文件的绝对路径
- `use_date`: 执行日期（例如：20260611）
- `mock`: 是否为测试模式（默认为 False，即真实执行）

## 2. 自动化执行步骤 (Pipeline Steps)
你必须**严格按照以下 4 个步骤的顺序**连续调用工具。
**不要在步骤之间停下来询问用户许可**，请在前置工具成功返回后，直接读取它的返回路径，并作为参数传递给下一个工具。

### Step 1: 提取 Hive 宽表特征 (Feature Extraction)
- **工具**: `run_spark_feature`
- **参数**: 传入 `user_name`, `file_path`, `use_date` 和 `mock`。
- **输出接力**: 该工具会返回 `sample_path`，请保存此路径供 Step 2 和 Step 4 使用。

### Step 2: 挂载线下店铺字典 (Knowledge Augmentation)
- **工具**: `augment_shop_knowledge`
- **参数**: 传入上一步获取的 `sample_path` 和 `mock`。
- **输出接力**: 该工具会返回 `sample_path_new`，请保存此路径供 Step 3 使用。

### Step 3: 唤醒子智能体并发推理 (Multi-agent Inference)
- **工具**: `run_worker_inference`
- **参数**: 传入上一步获取的 `sample_path_new` 和 `mock`。并发数 `num_workers` 可默认设置为 10。如果用户指定了推理模型（如 `gemini-2.5-pro`），请一并传入 `model_name` 参数。
- **输出接力**: 该工具会返回 `merged_jsonl_path`，请保存此路径供 Step 4 使用。

### Step 4: 生成最终 Excel 评测报告 (Report Generation)
- **工具**: `generate_excel_report`
- **参数**: 传入 Step 1 拿到的 `sample_path`，Step 3 拿到的 `merged_jsonl_path` (参数名为 `predict_jsonl`)，原始的 `file_path`，以及 `mock`。
- **输出**: 该工具会返回最终的 `result_details_path`。

## 3. 最终回复
当以上 4 步全部成功执行完毕后，将最终生成的 Excel 报告路径总结并发送给用户，宣布流水线执行成功。
