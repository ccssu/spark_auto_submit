import os
import logging
import paramiko
from tqdm import tqdm

logger = logging.getLogger(__name__)

class BaseNode:
    def __init__(self, hostname, port, username, password=None, private_key=None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.private_key = private_key

    def connect(self):
        raise NotImplementedError("connect() method must be implemented in the subclass")

    def execute_command(self, command):
        raise NotImplementedError("execute_command() method must be implemented in the subclass")

    def transfer_data(self, source_path, destination_path, callback=None):
        raise NotImplementedError("transfer_data() method must be implemented in the subclass")

    def _upload_file(self, sftp_client, local_file, remote_directory):
        file_size = os.path.getsize(local_file)
        file_size_mb = (file_size) / (1024 * 1024)

        # 打印开始信息
        logger.info(f"开始传输文件 {local_file} 到远程目录 {remote_directory}")
        logger.info(f"文件大小: {file_size_mb:.2f} MB")

        # 创建进度条对象 MB
        with tqdm(
            total=file_size_mb, desc="传输进度", unit="MB", unit_scale=True
        ) as progress_bar:

            def convert_bytes_to_mb(size_in_bytes):
                return size_in_bytes / (1024 * 1024)

            def callback(bytes_so_far, bytes_to_be_transferred):
                # progress_bar.update(bytes_so_far - progress_bar.n)
                progress_bar.update(
                    convert_bytes_to_mb(bytes_so_far) - progress_bar.n
                )
                progress_bar.set_postfix(
                    {"size": f"{convert_bytes_to_mb(bytes_so_far):.2f} MB"}
                )

            # 传输文件
            sftp_client.put(
                local_file,
                os.path.join(remote_directory, os.path.basename(local_file)),
                callback=callback,
            )
        return  os.path.join(remote_directory, os.path.basename(local_file))
        

    def _execute_command(self, transport, command):

        with transport.open_channel("session") as channel:
            channel.exec_command(command)
            output = channel.makefile().read()
            stderr = channel.makefile_stderr().read()
            exit_code = channel.recv_exit_status()

            return {
                'command': command,
                'stdout': output,
                'stderr': stderr,
                'exit_code': exit_code
            }
   