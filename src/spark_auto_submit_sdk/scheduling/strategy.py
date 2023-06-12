# scheduling/strategy.py
import logging
from abc import ABC, abstractmethod

# spark_auto_submit_sdk/src/core/utils/default_utils.py
from spark_auto_submit_sdk.core.utils.default_utils import performance_decorator

loger = logging.getLogger(__name__)


class SchedulingStrategy(ABC):
    @abstractmethod
    def schedule(self, tasks):
        return NotImplemented


# 提交到本地spark的任务
class LocalSchedulingStrategy(SchedulingStrategy):
    def schedule(self, tasks):
        # 打印调度任务信息
        loger.info("Performing local scheduling.")
        # 调度逻辑实现
        # 结果信息列表
        result_list = []
        for task in tasks:
            # 打印任务信息
            loger.info("Scheduling task: %s", task.name)
            # 调度逻辑实现
            with performance_decorator(task.name):
                result = task.run()
            # 将结果信息添加到结果信息列表
            result_list.append(result)
        # 返回结果信息列表
        return result_list


# 提交到远程主机spark的任务
class RemoteSchedulingStrategy(SchedulingStrategy):
    def schedule(self, tasks):
        # 打印调度任务信息
        loger.info("Performing Remote scheduling.")
        # 调度逻辑实现
        result_list = []
        for task in tasks:
            # 打印任务信息
            loger.info("Scheduling task: %s", task.name)
            # 调度逻辑实现
            with performance_decorator(task.name):
                if task.name == 'submit_command_task':
                    result = task.run_remote()
                else:
                    result = task.run()
            # 将结果信息添加到结果信息列表
            result_list.append(result)
        # 返回结果信息列表
        return result_list
