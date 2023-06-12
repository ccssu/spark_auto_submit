# 在 sdk/factory.py 文件中定义工厂类
from spark_auto_submit_sdk.scheduling.scheduler import Scheduler
from spark_auto_submit_sdk.monitoring.monitor import Monitoring
from spark_auto_submit_sdk.submission.submission_manager import SubmissionManager
from spark_auto_submit_sdk.core.hdfs_manager import HdfsManager
from spark_auto_submit_sdk.core.default_zip_manager import DefaultZipManager
from spark_auto_submit_sdk.core.configuration.validators import Validators
from spark_auto_submit_sdk.core.utils.file_utils import get_run_mode
from spark_auto_submit_sdk.scheduling.strategy import LocalSchedulingStrategy
from spark_auto_submit_sdk.scheduling.strategy import RemoteSchedulingStrategy

# /workspace/spark_auto_submit_sdk/src/submission/task.py
from spark_auto_submit_sdk.submission.task import PackageTask, BuildCommandTask, SubmitCommandTask


class SparkAutoSubmitSDKFactory:
    def create_scheduler(self, config):
        run_mode = get_run_mode(config)
        print("run_mode: ", run_mode)
        return Scheduler(
            LocalSchedulingStrategy()
            if "local" in run_mode
            else RemoteSchedulingStrategy()
        )

    # PackageTask
    def create_package_task(self, name, pack_config):
        return PackageTask(name, pack_config)

    # BuildCommandTask
    def create_build_command_task(self, name, sdk_config, project_package, params_dict):
        return BuildCommandTask(name, sdk_config, project_package, params_dict)

    # SubmitCommandTask
    def create_submit_command_task(
        self, name, submission_parameters: SubmitCommandTask
    ):
        return SubmitCommandTask(name, submission_parameters)

    def create_monitoring(self):
        return Monitoring()

    def create_submission_manager(self):
        return SubmissionManager()

    def create_hdfs_manager(self):
        return HdfsManager()

    def create_ssh_manager(self):
        return SSHManager()

    def create_default_zip_manager(self):
        return DefaultZipManager()


# # 在 sdk/spark_auto_submit_sdk.py 文件中使用工厂模式创建对象
# from factory import SparkAutoSubmitSDKFactory

# class SparkAutoSubmitSDK:
#     def __init__(self):
#         self.factory = SparkAutoSubmitSDKFactory()

#         self.scheduler = self.factory.create_scheduler()
#         self.monitoring = self.factory.create_monitoring()
#         self.submission_manager = self.factory.create_submission_manager()
#         self.hdfs_manager = self.factory.create_hdfs_manager()
#         self.ssh_manager = self.factory.create_ssh_manager()
#         self.default_zip_manager = self.factory.create_default_zip_manager()

#     # 其他方法和逻辑

# # 在 main.py 文件中实例化 SparkAutoSubmitSDK 并使用
# from sdk.spark_auto_submit_sdk import SparkAutoSubmitSDK

# def main():
#     sdk = SparkAutoSubmitSDK()
#     # 使用 sdk 中的对象进行调度、监控、提交等操作

# if __name__ == "__main__":
#     main()
