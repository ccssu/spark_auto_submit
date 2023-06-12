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
    def __init__(self, name, pack_config, chache=False):
        """pack_config
        project_dir = pack_config.get('project_dir')
        python_env = pack_config.get('python_env')
        datasets_dir = pack_config.get('datasets_dir',None)
        """
        super().__init__(name)
        self.pack_config = pack_config
        self.chache = chache

    def __default_output_path(self, output_path):
        save_name = os.path.basename(output_path)
        parent_dir = os.path.dirname(output_path)
        output_path = os.path.join(parent_dir, save_name + ".zip")

        return output_path

    def __remove_file(self, file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
            print("remove file: ", file_path)

    def run(self):
        super().run()
        print("PackageTask run")
        pack_config = self.pack_config
        project_dir = pack_config.get("project_dir")
        python_env = pack_config.get("python_env")
        datasets_dir = pack_config.get("datasets_dir", None)
        # 断言
        # assert project_dir is not None, "project_dir is None"
        assert python_env is not None, "python_env is None"

        if project_dir:
            with working_directory(project_dir):
                zip_worker = DefaultZipManager(project_dir)
                file_list = get_non_ignored_files(project_dir)
                file_list = [
                    os.path.relpath(file_path, project_dir) for file_path in file_list
                ]
                output_path = self.__default_output_path(project_dir)
                project_zip_path = output_path
                if not self.chache:
                    self.__remove_file(output_path)

                if os.path.exists(output_path):
                    project_zip_path = output_path
                else:
                    project_zip_path = zip_worker.zip_code_files(
                        file_list, output_path, description="压缩项目代码.zip"
                    )

        if python_env:
            with working_directory(python_env):
                zip_worker = DefaultZipManager(python_env)
                output_path = self.__default_output_path(project_dir)
                python_zip_path = output_path
                if not self.chache:
                    self.__remove_file(output_path)

                if os.path.exists(output_path):
                    python_zip_path = output_path
                else:
                    python_zip_path = zip_worker.zip_directory(
                        python_env, output_path, description="压缩python环境.zip"
                    )

        result = {
            "project_zip_path": project_zip_path,
            "python_zip_path": python_zip_path,
        }

        if datasets_dir is not None:
            with working_directory(datasets_dir):
                zip_worker = DefaultZipManager(datasets_dir)

                output_path = self.__default_output_path(project_dir)
                if not self.chache:
                    self.__remove_file(output_path)
                # 文件存在
                if os.path.exists(output_path):
                    result["datasets_zip_path"] = output_path
                else:
                    result["datasets_zip_path"] = zip_worker.zip_directory(
                        datasets_dir, output_path, description="压缩dataset.zip"
                    )

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
    def __init__(self, name, submission_parameters: SubmissionParameters, node=None):
        super().__init__(name)  # node 为提交的节点信息
        self.submission_parameters = submission_parameters
        self.node = node 

    def run(self):
        super().run()
        local_command = self.submission_parameters.to_spark_submit_command()
        execute_command(local_command)

    # 远程运行
    def _default_name(self, romte_dir, local_file):
        file_name = get_file_basename(local_file)
        output = os.join(romte_dir, file_name)
        return output

    def run_remote(self):
        # 上传文件
        main_file = self.submission_parameters.main_file
        remote_dir = "/data/guangnian/temp"
        self.node.transfer_data(main_file, remote_dir)
        self.submission_parameters.main_file = self._default_name(remote_dir, main_file)
        # 上传文件
        python_env = self.submission_parameters.project_package.get("python_zip_path")
        self.node.transfer_data(python_env, remote_dir)
        self.submission_parameters.project_package[
            "python_zip_path"
        ] = self._default_name(remote_dir, python_env)

        hadoop = "/usr/local/service/hadoop/bin/hadoop"
        
        execute_callback = lambda x: print(x)
        python_zip_path = self.submission_parameters.project_package.get(
            "python_zip_path"
        )
        self.node.execute_command(
            f"{hadoop} fs -put {python_zip_path} /py_env/{get_file_basename(python_zip_path)}",
            execute_callback,
        )

        # 上传文件
        project_dir = self.submission_parameters.project_package.get("project_zip_path")
        self.node.transfer_data(project_dir, remote_dir)
        self.submission_parameters.project_package[
            "project_zip_path"
        ] = self._default_name(remote_dir, project_dir)
        # 上传文件
        datasets_dir = self.submission_parameters.project_package.get(
            "datasets_zip_path"
        )
        self.node.transfer_data(datasets_dir, remote_dir)
        self.submission_parameters.project_package[
            "datasets_zip_path"
        ] = self._default_name(remote_dir, datasets_dir)

        cmd = self.submission_parameters.to_spark_submit_command()

        self.node.execute_command(cmd)

    # node.transfer_data(zip_path, '/data/guangnian/temp')
    # # result = node.transfer_data('/workspace/DEMO/test_project00/pyspark_venv.tar.gz', '/data/guangnian/temp')
    # # destination_file = '/data/guangnian/temp/pyspark_venv.tar.gz'
    # # node.execute_command('ls -l /data/guangnian/temp', execute_callback)
    # destination_file = '/data/guangnian/temp/pyspark_venv.zip'
    # base_name = os.path.basename(destination_file)
    # hadoop = '/usr/local/service/hadoop/bin/hadoop'
    # node.execute_command(f'{hadoop} fs -put {destination_file} /py_env/{base_name}', execute_callback)
    # # node.execute_command(f'{hadoop} fs -ls /py_env/', execute_callback)
