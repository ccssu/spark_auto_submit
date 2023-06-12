#!/bin/bash 
# bash script to format the project using Black
# 执行指令 bash ./scripts/format_project.sh
# 适用 OneLab 的 environment
source deactivate && source deactivate 
# 获取当前脚本所在的目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 构建spark_auto_submit_sdk项目目录路径
# 上级目录 
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo $PROJECT_DIR
# 使用Black格式化项目中的所有文件

python -m pip install black

python -m black "$PROJECT_DIR"
