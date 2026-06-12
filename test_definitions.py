import os
import sys
from cli import HermesCLI

cli = HermesCLI(
    skills=["grocery-labeling-pipeline"],
    model="qwen-plus",
    provider="custom",
    api_key="fake",
    base_url="fake",
    quiet=True
)

# Agent is initialized in _init_agent
cli._init_agent()
print("Enabled toolsets:", cli.enabled_toolsets)

from model_tools import get_tool_definitions
tools = get_tool_definitions(enabled_toolsets=cli.enabled_toolsets, quiet_mode=True)
tool_names = [t["function"]["name"] for t in tools]
print("Tool names:", tool_names)
if "run_spark_feature" in tool_names:
    print("YES, run_spark_feature is exposed to the model!")
else:
    print("NO, run_spark_feature is NOT exposed!")

