# file_utils.py:

# 类名: N/A
# 作用: 提供与文件操作相关的实用函数或类方法。
import yaml
import logging
from pathlib import Path

loger = logging.getLogger(__name__)


def load_yaml_config(config_file):
    # 读取配置文件
    if isinstance(config_file, str) or isinstance(config_file, Path):
        with open(config_file, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
    elif isinstance(config, dict):
        pass
    else:
        raise TypeError("logging config must be dict or str")


# 得到运行模式 local 还是 yarn
def get_run_mode(config):
    try:
        spark_submit_args = config["SPARK_SUBMIT"].get("spark_submit_args", None)
        spark_submit_mode = [
            arg for arg in spark_submit_args if arg.get("--master", None) is not None
        ]
        if len(spark_submit_mode) != 1:
            raise ValueError("Missing --master configuration")
        spark_submit_mode = spark_submit_mode[0].get("--master", None)
        return spark_submit_mode
    except Exception as e:
        logger.error("Error getting run mode: %s", str(e))
        raise e
