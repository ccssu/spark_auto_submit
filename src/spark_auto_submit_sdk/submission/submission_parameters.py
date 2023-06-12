# submission_parameters.py:

# 类名: SubmissionParameters
# 作用: 定义应用程序提交参数的数据结构，包括应用程序名称、主文件路径、应用程序参数等。

import os
import copy
from typing import Any, Dict, List


class SubmissionParameters:
    def __init__(self, sdk_config, project_package, params_dict):
        sdk_config = copy.deepcopy(sdk_config)
        project_package = copy.deepcopy(project_package)
        params_dict = copy.deepcopy(params_dict)
        self.project_package = project_package

        self.spark_submit_home = sdk_config.get("spark-submit")
        self.spark_submit_args = sdk_config.get("spark_submit_args")
        self.app_name = params_dict.get("app_name", "test_app")
        self.spark_submit_args.append({"--name": self.app_name})
        self.main_file = params_dict.get("main_file")
        self.args = params_dict.get("args", [])

    def __join_values(self, data):
        key = next(iter(data.keys()))
        values = data[key]
        values = (
            [str(value) for value in values]
            if isinstance(values, list)
            else [str(values)]
        )
        return " ".join([f"{key} {value}" for value in values])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "spark_submit_home": self.spark_submit_home,
            "spark_submit_args": self.spark_submit_args,
            "app_name": self.app_name,
            "main_file": self.main_file,
            "args": self.args,
        }

    # 生产spark-submit依赖包
    def __to_spark_submit_package(self) -> str:
        project_package = self.project_package
        package_list = []
        package_list.append({"--py-files": project_package.get("project_zip_path")})
        package_list.append(
            {"--archives": f"{project_package.get('python_zip_path')}#PY3"}
        )
        python_zip_path = project_package.get("python_zip_path")
        # 去掉文件.zip后缀
        python_zip = os.path.basename(python_zip_path)
        python_zip = python_zip.split(".")[0]
        package_list.append(
            {
                "--conf": f"spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/{python_zip}/bin/python"
            }
        )
        return package_list

    # 生成spark-submit命令
    def to_spark_submit_command(self) -> str:
        # spark_submit_args.append({"--py-files": project_package.get('project_zip_path')})
        # spark_submit_args.append({"--archives": f"{project_package.get('python_zip_path')}#PY3"})
        # spark_submit_args.append({"--conf": "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/bin/python"})
        package_list = self.__to_spark_submit_package()
        spark_submit_args = [
            self.__join_values(data) for data in self.spark_submit_args
        ]
        for package in package_list:
            spark_submit_args.append(self.__join_values(package))

        args = [self.__join_values(arg) for arg in self.args]
        submit_command = (
            self.spark_submit_home
            + " \\\n"
            + " \\\n".join(spark_submit_args)
            + " \\\n"
            + self.main_file 
            + ( (" \\\n" + " \\\n".join(args)) if len(args) > 0 else  "\n")
        )
        return submit_command
