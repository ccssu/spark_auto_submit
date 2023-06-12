import logging


class LoggingUtils:
    @staticmethod
    def get_logger(name):
        return logging.getLogger(name)

    @staticmethod
    def configure_logging(config):
        # 解析配置参数
        log_level = config.get("log_level", logging.INFO)
        log_format = config.get(
            "log_format", "%(asctime)s - %(levelname)s - %(message)s"
        )
        log_file = config.get("log_file")

        # 创建全局日志记录器
        logger = logging.getLogger()
        logger.setLevel(log_level)

        # 日志处理器
        handlers = config.get("handlers", [])
        # console 是否输出到控制台
        is_console = "console" in handlers
        # file 是否输出到文件
        is_file = "file" in handlers

        if is_file and log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            formatter = logging.Formatter(log_format)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        if is_console:
            # 创建控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            formatter = logging.Formatter(log_format)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger
