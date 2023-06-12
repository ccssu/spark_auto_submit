# submission_parameters.py:

# 类名: SubmissionParameters
# 作用: 定义应用程序提交参数的数据结构，包括应用程序名称、主文件路径、应用程序参数等。

import copy
from typing import Any, Dict, List


class SubmissionParameters:
    def __init__(self, sdk_config, project_package, params_dict):
        sdk_config = copy.deepcopy(sdk_config)
        project_package = copy.deepcopy(project_package)
        params_dict = copy.deepcopy(params_dict)

        self.spark_submit_home = sdk_config.get("spark-submit")
        self.spark_submit_args = sdk_config.get("spark_submit_args")
        self.spark_submit_args.append(
            {"--py-files": project_package.get("project_zip_path")}
        )
        self.spark_submit_args.append(
            {"--archives": f"{project_package.get('python_zip_path')}#PY3"}
        )
        self.spark_submit_args.append(
            {"--conf": "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/bin/python"}
        )
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

    # 生成spark-submit命令
    def to_spark_submit_command(self) -> str:
        # spark_submit_args.append({"--py-files": project_package.get('project_zip_path')})
        # spark_submit_args.append({"--archives": f"{project_package.get('python_zip_path')}#PY3"})
        # spark_submit_args.append({"--conf": "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/bin/python"})
        spark_submit_args = [
            self.__join_values(data) for data in self.spark_submit_args
        ]
        args = [self.__join_values(arg) for arg in self.args]
        submit_command = (
            self.spark_submit_home
            + " \\\n"
            + " \\\n".join(spark_submit_args)
            + " \\\n"
            + self.main_file
            + " \\\n"
            + " \\\n".join(args)
        )
        return submit_command
