import os
import copy
from contextlib import contextmanager
import logging
import subprocess
import time
from contextlib import contextmanager
from fabric import Connection
from gitignore_parser import parse_gitignore

logger = logging.getLogger(__name__)

__all__ = [
    "deep_copy_object",
    "execute_command",
    "get_file_basename",
    "get_non_ignored_files",
    "working_directory",
    "print_directory_tree",
    "test_ssh_connection",
    "get_local_ip",
]


def deep_copy_object(obj):
    return copy.deepcopy(obj)


@contextmanager
def performance_decorator(name):
    start_time = time.time()
    yield
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Function {name} executed in {execution_time} seconds")


# 执行Bash命令并将输出传递给日志记录
def execute_command(command):
    try:
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        for line in iter(process.stdout.readline, b""):
            logging.info(line.decode().strip())
        process.stdout.close()
        process.wait()
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Command execution failed: {e.returncode} - {e.output.decode().strip()}"
        )


def get_file_basename(file_path):
    return os.path.basename(os.path.normpath(file_path))


def get_non_ignored_files(root_dir):
    # Path to the .gitignore file
    gitignore_path = os.path.join(root_dir, ".gitignore")

    # Parse the .gitignore file
    gitignore = parse_gitignore(gitignore_path)

    non_ignored_files = []

    for root, dirs, files in os.walk(root_dir):
        # Exclude directories that are ignored by .gitignore
        dirs[:] = [d for d in dirs if not gitignore(os.path.join(root, d))]

        # Exclude files that are ignored by .gitignore
        non_ignored_files.extend(
            os.path.join(root, file)
            for file in files
            if not gitignore(os.path.join(root, file))
        )

    return non_ignored_files


@contextmanager
def working_directory(path):
    """上下文管理器，将工作目录切换到指定路径，执行完成后还原到原始目录"""
    current_dir = os.getcwd()  # 获取当前工作目录
    try:
        os.chdir(path)  # 切换工作目录
        yield
    finally:
        os.chdir(current_dir)  # 还原工作目录


def print_directory_tree(root_path, level=0, max_level=999, indent=""):
    if level > max_level:
        return

    items = os.listdir(root_path)
    for index, item in enumerate(sorted(items)):
        item_path = os.path.join(root_path, item)
        is_last_item = index == len(items) - 1

        if is_last_item:
            marker = "└── "
            new_indent = indent + "    "
        else:
            marker = "├── "
            new_indent = indent + "│   "

        logger.info(indent + marker + item)

        if os.path.isdir(item_path):
            print_directory_tree(
                item_path, level=level + 1, max_level=max_level, indent=new_indent
            )


def test_ssh_connection(hostname, username, password):
    try:
        # 建立SSH连接
        c = Connection(
            host=hostname,
            user=username,
            connect_kwargs={"password": password},
            connect_timeout=10,
        )

        # 执行远程命令，这里我们以执行"ls"命令为例
        result = c.run("ls", hide=True)

        # 检查命令执行结果
        if result.ok:
            logger.info("SSH连接测试成功！")
        else:
            logger.info("SSH连接测试失败！")

        # 关闭SSH连接
        c.close()
    except Exception as e:
        logger.info(f"SSH连接测试失败：{str(e)}")


def get_local_ip():
    # 获取本机主机名
    hostname = socket.gethostname()
    # 获取本机IP地址
    ip_address = socket.gethostbyname(hostname)
    return ip_address


if __name__ == "__main__":

    # Example usage for a directory
    root_path = "/workspace/pyspark_venv"
    print_directory_tree(root_path, max_level=0)
    # Example usage for a zip archive
