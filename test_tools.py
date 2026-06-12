import sys
from tools.registry import discover_builtin_tools, registry

print("Importing explicit tools...")
try:
    import tools.run_spark_feature_tool
    import tools.augment_shop_knowledge_tool
except Exception as e:
    print(f"Import error: {e}")

print("Tools in registry:", list(registry._tools.keys()))
