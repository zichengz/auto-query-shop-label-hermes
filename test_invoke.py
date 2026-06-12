from tools.registry import registry
try:
    import tools.run_spark_feature_tool
    import tools.augment_shop_knowledge_tool
except Exception as e:
    print(e)

print(registry.dispatch("run_spark_feature", {"user_name": "guoshubin", "file_path": "/dev/null", "use_date": "20260611", "mock": True}))
