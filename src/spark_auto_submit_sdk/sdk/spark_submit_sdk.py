# spark_auto_submit_sdk.py:

# 类名: SparkAutoSubmitSDK
# 作用: 提供整个SDK的公共接口和功能，包括配置SDK、创建提交参数、提交应用程序、监控应用程序等。
# 在 sdk/ 目录下创建 sdk.py 文件

# 导入其他模块
from .factory import SparkAutoSubmitSDKFactory
from spark_auto_submit_sdk.core.utils.logging_utils import LoggingUtils
from spark_auto_submit_sdk.core.configuration.validators import Validators

from spark_auto_submit_sdk.core.configuration.settings import Settings

# 创建 SparkAutoSubmitSDK 类

__all__ = ["SparkAutoSubmitSDK"]


class SparkAutoSubmitSDK:
    def __init__(self, config=Settings.DEFAULT_CONFIG_FILE):
        self.factory = SparkAutoSubmitSDKFactory()
        self.configure(config)

    def configure(self, config):
        # 验证配置文件
        config = Validators.validate_config(config)
        self.config = config
        # 其他初始化代码
        self.logger = LoggingUtils.configure_logging(
            config.get("LOGGING", Settings.DEFAULT_CONFIG_FILE)
        )
        self.logger.info("Initializing SparkAutoSubmitSDK...")
        # 创建调度器、监控器、提交管理器对象
        self.scheduler = self.factory.create_scheduler(config)
        # self.submission_manager = SubmissionManager()
        # self.submission_manager = self.factory.create_submission_manager(config)
        # self.monitoring = Monitoring()
        # self.monitoring = self.factory.create_monitoring(config)

        # # 创建核心模块对象
        # self.hdfs_manager = HdfsManager(config["HDFS"])
        # self.ssh_manager = SSHManager(config["SSH"])
        # self.default_zip_manager = DefaultZipManager()

        # # 注册观察者
        # self.submission_manager.register_observer(self.monitoring)

    def __pack_project(self, chache=False):
        # 打包项目
        package_task = self.factory.create_package_task(
            "package_task", self.config.get("LOCAL_PROJECT"), chache=chache
        )
        return self.__single_execute_task(package_task)

        # 执行单个任务 保证任务队列为空

    def __single_execute_task(self, task):
        result_list = self.scheduler.schedule_task(task)
        return result_list[0]

    def create_submission_parameters(self, app_name, main_file, args, *, chache=True):

        sdk_config = self.config.get("SPARK_SUBMIT")
        project_package = self.__pack_project(chache=chache)

        params_dict = {"app_name": app_name, "main_file": main_file, "args": args}
        # 构建指令任务
        build_command_task = self.factory.create_build_command_task(
            "build_command_task", sdk_config, project_package, params_dict
        )
        submission_parameters = self.__single_execute_task(build_command_task)
        return submission_parameters

    def submit_application(self, submission_parameters, node=None):
        # 提交任务
        submit_command_task = self.factory.create_submit_command_task(
            "submit_command_task", submission_parameters,node=node
        )
        return self.__single_execute_task(submit_command_task)

    def retrieve_application_logs(self, application_id):
        pass

    def get_application_status(self, application_id):
        pass

    def add_task(self, task):
        self.scheduler.add_task(task)

    def remove_task(self, task):
        self.scheduler.remove_task(task)

    def schedule_tasks(self):
        result_list = self.scheduler.schedule_tasks()
        self.scheduler.remove_all_tasks()
        return result_list

        # self.monitor.perform_monitoring()

    # # 提交任务
    # def submit_task(self, task):
    #     try:
    #         # 调度任务
    #         job = self.scheduler.schedule_task(task)

    #         # 提交任务
    #         self.submission_manager.submit_job(job)
    #     except Exception as e:
    #         self.logger.error("Error submitting task: %s", str(e))
    #         # 错误处理逻辑

    # # 监控任务
    # def monitor_task(self, task):
    #     self.monitoring.monitor_task(task)

    # # 其他方法和逻辑

    # def create_submission_parameters(self, app_name, main_file, args):
    #     return self.factory.create_submission_parameters(task, self.config)

    # def submit_application(self, submission_parameters):
    #     return self.factory.submit_application(submission_parameters, self.config)


# # 在 main.py 文件中使用 SparkAutoSubmitSDK
# def main():
#     # 加载配置文件
#     config = load_config("config.yaml")

#     # 创建 SparkAutoSubmitSDK 实例
#     sdk = SparkAutoSubmitSDK(config)

#     # 设置调度策略
#     sdk.set_scheduling_strategy("RoundRobinStrategy")

#     # 创建任务
#     task1 = Task("Task 1")
#     task2 = Task("Task 2")

#     # 提交任务
#     sdk.submit_task(task1)
#     sdk.submit_task(task2)

#     # 监控任务
#     sdk.monitor_task(task1)
#     sdk.monitor_task(task2)

# if __name__ == "__main__":
#     main()
