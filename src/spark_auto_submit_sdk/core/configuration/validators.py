"""验证器
是用于检验配置项的合法性的工具
"""
import yaml
from pathlib import Path


class Validators:
    @staticmethod
    def load_yaml_config(config):
        # 读取配置文件
        if isinstance(config, str) or isinstance(config, Path):
            with open(config, "r") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        elif isinstance(config, dict):
            pass
        else:
            raise TypeError("logging config must be dict or str")
        return config

    @staticmethod
    def validate_config(config):
        config = Validators.load_yaml_config(config)

        # 其他校验逻辑
        if config.get("SPARK_SUBMIT", None) is None:
            raise ValueError("Missing SPARK_SUBMIT configuration")

        spark_submit_args = config["SPARK_SUBMIT"].get("spark_submit_args", None)

        if spark_submit_args is None:
            raise ValueError("Missing spark_submit_args configuration")

        spark_submit_mode = [
            arg for arg in spark_submit_args if arg.get("--master", None) is not None
        ]
        if len(spark_submit_mode) != 1:
            raise ValueError("Missing --master configuration")
        spark_submit_mode = spark_submit_mode[0].get("--master", None)

        if "local" not in spark_submit_mode and "yarn" not in spark_submit_mode:
            raise ValueError("Invalid --master configuration")

        if config.get("LOCAL_PROJECT", None) is None:
            raise ValueError("Missing LOCAL_PROJECT configuration")

        if "yarn" in spark_submit_mode:
            # SSH 配置
            if config.get("SSH", None) is None:
                raise ValueError("Missing SSH configuration in yarn mode")

            # HDFS
            if config.get("HDFS", None) is None:
                raise ValueError("Missing HDFS configuration in yarn mode")

        return config
