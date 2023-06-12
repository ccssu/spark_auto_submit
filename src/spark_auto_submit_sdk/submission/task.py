from abc import ABC, abstractmethod
import os
from spark_auto_submit_sdk.core.default_zip_manager import DefaultZipManager

from spark_auto_submit_sdk.core.utils.default_utils import (
    execute_command,
    get_file_basename,
    get_non_ignored_files,
    working_directory,
)
from .submission_parameters import SubmissionParameters

__all__ = ["PackageTask", "BuildCommandTask", "SubmitCommandTask"]


class Task(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def run(self, *args, **kwargs):
        # 任务执行逻辑
        pass


# 打包项目包 任务
class PackageTask(Task):
    def __init__(self, name, pack_config):
        """pack_config
        project_dir = pack_config.get('project_dir')
        python_env = pack_config.get('python_env')
        datasets_dir = pack_config.get('datasets_dir',None)
        """
        super().__init__(name)
        self.pack_config = pack_config

    def run(self):
        super().run()
        print("PackageTask run")
        pack_config = self.pack_config
        project_dir = pack_config.get("project_dir")
        python_env = pack_config.get("python_env")
        datasets_dir = pack_config.get("datasets_dir", None)
        # 断言
        assert project_dir is not None, "project_dir is None"
        assert python_env is not None, "python_env is None"

        # 打包
        with working_directory(project_dir):
            zip_worker = DefaultZipManager(project_dir)
            file_list = get_non_ignored_files(project_dir)
            file_list = [
                os.path.relpath(file_path, project_dir) for file_path in file_list
            ]
            print(f"file_list: {(file_list)}")
            save_name = get_file_basename(project_dir) + "_code.zip"
            # 压缩 code.zip
            project_zip_path = zip_worker.zip_files(
                file_list, project_dir, save_name, "压缩项目代码.zip"
            )

        with working_directory(python_env):
            zip_worker = DefaultZipManager(python_env)
            file_list = zip_worker.get_dir_files()
            # 排除掉不需要的文件 *.zip
            file_list = [
                file_path
                for file_path in file_list
                if not file_path.endswith("_python_env.zip")
            ]
            save_name = get_file_basename(python_env) + "_python_env.zip"
            python_zip_path = zip_worker.zip_files(
                file_list, python_env, save_name, "压缩python环境.zip"
            )

        result = {
            "project_zip_path": project_zip_path,
            "python_zip_path": python_zip_path,
        }
        if datasets_dir is not None:
            with working_directory(datasets_dir):
                zip_worker = DefaultZipManager(datasets_dir)
                file_list = zip_worker.get_dir_files()
                # 排除掉不需要的文件 *.zip
                file_list = [
                    file_path
                    for file_path in file_list
                    if not file_path.endswith("_datasets.zip")
                ]
                save_name = get_file_basename(datasets_dir) + "_datasets.zip"
                datasets_zip_path = zip_worker.zip_files(
                    file_list, python_env, save_name, "压缩datasets.zip"
                )
                result["datasets_zip_path"] = datasets_zip_path

        return result


# 构建指令 任务
class BuildCommandTask(Task):
    def __init__(
        self, name, sdk_config: dict, project_package: dict, params_dict: dict
    ):
        super().__init__(name)
        self.sdk_config = sdk_config
        self.project_package = project_package
        self.params_dict = params_dict

    def run(self):
        super().run()
        return SubmissionParameters(
            self.sdk_config, self.project_package, self.params_dict
        )


# 提交指令 任务
class SubmitCommandTask(Task):
    def __init__(self, name, submission_parameters: SubmissionParameters):
        super().__init__(name)
        self.submission_parameters = submission_parameters

    def run(self):
        super().run()
        local_command = self.submission_parameters.to_spark_submit_command()
        execute_command(local_command)
