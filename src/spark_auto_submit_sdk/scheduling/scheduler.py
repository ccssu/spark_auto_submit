# scheduler.py:

# 类名: Scheduler
# 作用: 调度应用程序的执行，包括将应用程序添加到作业队列、管理作业队列等。

# scheduling/scheduler.py

from .strategy import SchedulingStrategy


class Scheduler:
    def __init__(self, scheduling_strategy: SchedulingStrategy):
        self.tasks = []
        self.scheduling_strategy = scheduling_strategy

    def add_task(self, task):
        self.tasks.append(task)

    def remove_task(self, task):
        self.tasks.remove(task)

    # 删除全部任务
    def remove_all_tasks(self):
        self.tasks = []

    # 删除多个任务
    def remove_tasks(self, tasks):
        for task in tasks:
            self.tasks.remove(task)

    # 调度单个任务
    def schedule_task(self, task):
        return self.scheduling_strategy.schedule([task])

    def schedule_tasks(self):
        return self.scheduling_strategy.schedule(self.tasks)
