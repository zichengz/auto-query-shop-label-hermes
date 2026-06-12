from model_tools import get_tool_definitions
from hermes_cli.tools_config import _get_platform_tools
from hermes_cli.config import load_config
toolsets = _get_platform_tools(load_config(), "cli")
tools = get_tool_definitions(enabled_toolsets=toolsets, quiet_mode=True)
names = [t["function"]["name"] for t in tools]
print("run_spark_feature in tools:", "run_spark_feature" in names)
print("Toolsets:", toolsets)
