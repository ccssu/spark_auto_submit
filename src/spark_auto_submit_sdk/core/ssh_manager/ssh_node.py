import time
import paramiko
from sshtunnel import SSHTunnelForwarder
import logging
import os
import socket

import paramiko
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm
logger = logging.getLogger(__name__)
from .common import BaseNode
__all__ = ['SSHNode']
class SSHNode(BaseNode):
    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __init__(self, hostname, port=22, username=None, password=None, private_key=None):
        super().__init__(hostname, port, username, password, private_key)
        self.ssh_client = None

    def connect(self):
        if self.ssh_client is not None:
            raise Exception("SSH connection already established")

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.private_key is not None:
            self.ssh_client.connect(self.hostname, self.port, self.username, pkey=self.private_key)
        else:
            self.ssh_client.connect(self.hostname, self.port, self.username, self.password)

    def disconnect(self):
        if self.ssh_client is not None:
            self.ssh_client.close()
            self.ssh_client = None
  
    def execute_command(self, command, callack=None):
        if self.ssh_client is None:
            raise Exception("SSH connection not established")
        transport = self.ssh_client.get_transport()
        result = self._execute_command(transport, command)
        if callack:
            callack(result)
        return result
    
    def transfer_data(self, source_path, destination_path):
        if self.ssh_client is None:
            raise Exception("SSH connection not established")
        # 注意暂时只能上传文件，不能上传文件夹
        check_file = os.path.isfile(source_path)
        logger.warning(f"注意暂时只能上传文件，不能上传文件夹, 传入的路径是{source_path} 是否是文件：{check_file}")
        
        sftp_client = self.ssh_client.open_sftp()
        self.__upload_file(sftp_client, source_path, destination_path)
        sftp_client.close()
        
    def download_file(self, remote_file, local_path):
        if self.sftp_client:
            self.sftp_client.get(remote_file, local_path)
        else:
            raise Exception("SFTP connection is not established.")

    def create_remote_directory(self, remote_directory):
        if self.sftp_client:
            self.sftp_client.mkdir(remote_directory)
        else:
            raise Exception("SFTP connection is not established.")

    def delete_remote_file(self, remote_file):
        if self.sftp_client:
            self.sftp_client.remove(remote_file)
        else:
            raise Exception("SFTP connection is not established.")

    def delete_remote_directory(self, remote_directory):
        if self.sftp_client:
            self.sftp_client.rmdir(remote_directory)
        else:
            raise Exception("SFTP connection is not established.")

    def list_remote_files(self, remote_directory):
        if self.sftp_client:
            files = self.sftp_client.listdir(remote_directory)
            return files
        else:
            raise Exception("SFTP connection is not established.")

    def get_remote_file_size(self, remote_file):
        if self.sftp_client:
            file_attr = self.sftp_client.stat(remote_file)
            size = file_attr.st_size
            return size
        else:
            raise Exception("SFTP connection is not established.")

    def check_remote_file_exists(self, remote_file):
        if self.sftp_client:
            try:
                self.sftp_client.stat(remote_file)
                return True
            except FileNotFoundError:
                return False
        logger.error("SFTP connection is not established.")
        raise Exception("SFTP connection is not established.")


