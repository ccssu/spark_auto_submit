# spark_auto_submit
自动化允许您从本地节点轻松地在集群的任意节点上启动 Spark 应用程序。


# install 


# 使用 SDK python

```python
from spark_auto_submit_sdk import SparkAutoSubmitSDK

#  配置 SDK，例如设置认证信息和连接参数 的yaml文件
config_file = "your_config.yaml"

# 创建 SparkAutoSubmitSDK 实例
sdk = SparkAutoSubmitSDK(config_file)


# 创建提交参数，指定应用程序名称、主文件和参数
app_name = "MySparkApp"
main_file = "main.py"
args = ["arg1", "arg2"]
# 创建提交参数，指定应用程序名称、主文件和参数
app_name = "MySparkApp"
main_file = "application.py"
# 注意：args 传入参数时， --args 不能省略 , 如果只是 --no-save 没有后面的参数
args = [{"--idf_path":"data/idf.txt"},
        {"--idx_path":"data/idx.txt"},
        {"--model_path":"data/model"},
        {"--raw_path":"data/sample_toutiao_cat_data.txt"},
        {"--char_emb_path":"data/char_emb.txt"},
        {"--result_path":"result.txt"},
        {"--no-save":" "} 
        ]
        
submission_params = sdk.create_submission_parameters(app_name, main_file, args)

# 提交应用程序
application_id = sdk.submit_application(submission_params)
print("Submitted Application ID:", application_id)

# 获取应用程序日志
logs = sdk.retrieve_application_logs(application_id)
print("Application Logs:", logs)
```
