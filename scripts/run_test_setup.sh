#!/bin/bash
source deactivate && source deactivate 
# 获取当前脚本所在的目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 构建spark_auto_submit_sdk项目目录路径
# 上级目录 
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd $PROJECT_DIR

# 创建虚拟环境
python -m venv myenv 

source myenv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 运行测试
python main.py

# 清理虚拟环境
deactivate

rm -rf myenv
