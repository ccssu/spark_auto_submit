# settings.py:

# 类名: Settings
# 作用: 定义SDK的配置设置，如认证信息、连接参数等。

# LOGGING:
#   log_level: INFO
#   log_format: '%(asctime)s - %(levelname)s - %(message)s'
#   handlers:  # handlers  are  used  to  define  where  the  logs  should  go
#     - console
#     - file
#   log_file: app.log #  the  name  of  the  log  file defined  is app.log


class Settings:
    # 默认配置文件路径
    DEFAULT_CONFIG_FILE = "~/.spark_auto_submit_sdk/config.yaml"

    DEFAULT_LOGGING_CONFIG = {
        "log_level": "INFO",
        "log_format": "%(asctime)s - %(levelname)s - %(message)s",
        "handlers": ["console", "file"],
        "log_file": "app.log",
    }


# import os
# import logging
# import yaml
# from pathlib import Path

# # from py_spark_sdk.utils import print_directory_tree, test_ssh_connection
# logger = logging.getLogger(__name__)

# def setup_logging(config):
#     # 读取配置文件
#     if isinstance(config, str) or isinstance(config, Path):
#         with open(config, 'r') as f:
#             config = yaml.load(f, Loader=yaml.FullLoader)
#     elif isinstance(config, dict):
#         pass
#     else:
#         raise TypeError('logging config must be dict or str')
#     # 解析配置参数
#     log_level = config.get('log_level', logging.INFO)
#     log_format = config.get('log_format', '%(asctime)s - %(levelname)s - %(message)s')
#     log_file = config.get('log_file')

#     # 创建全局日志记录器
#     logger = logging.getLogger()
#     logger.setLevel(log_level)

#     # 日志处理器
#     handlers = config.get('handlers', [])
#     # console 是否输出到控制台
#     is_console = 'console' in handlers
#     # file 是否输出到文件
#     is_file = 'file' in handlers

#     if is_file and log_file:
#         file_handler = logging.FileHandler(log_file)
#         file_handler.setLevel(log_level)
#         formatter = logging.Formatter(log_format)
#         file_handler.setFormatter(formatter)
#         logger.addHandler(file_handler)

#     if is_console:
#         # 创建控制台处理器
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(log_level)
#         formatter = logging.Formatter(log_format)
#         console_handler.setFormatter(formatter)
#         logger.addHandler(console_handler)

# def setup_ssh_config(config):
#     test_ssh_connection(config.get('host'), config.get('username'), config.get('password'))


# def initconfig(default_config):
#     logger.info('Loading config from %s', default_config)
#     if isinstance(default_config, str) or isinstance(default_config, Path):
#         with open(default_config, 'r') as f:
#            default_config = yaml.load(f, Loader=yaml.FullLoader)
#     setup_logging(default_config.get('LOGGING'))
#     setup_ssh_config(default_config.get('SSH'))
#     return default_config

# # INIT_CONFIG = initconfig( '/workspace/spark_tool/config/development.yaml')


# def load_config(config_file):
#     # 初始化配置
#     config = initconfig(config_file)
#     return config


# if __name__ == '__main__':
#     logging.debug('This is a debug log message')
#     logging.info('This is an info log message')
#     logging.warning('This is a warning log message')
