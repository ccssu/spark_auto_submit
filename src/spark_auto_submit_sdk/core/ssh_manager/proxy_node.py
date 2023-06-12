import time
import paramiko
from sshtunnel import SSHTunnelForwarder
import logging
import os
import socket

import paramiko
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from .common import BaseNode

__all__ = ["ProxyNode"]


class ProxyNode(BaseNode):
    def __init__(self, ssh_node1, ssh_node2):
        self.ssh_node1 = ssh_node1
        self.ssh_node2 = ssh_node2
        self.tunnel = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        self.ssh_node1.connect()
        self.tunnel = SSHTunnelForwarder(
            (self.ssh_node1.hostname, self.ssh_node1.port),
            ssh_username=self.ssh_node1.username,
            ssh_password=self.ssh_node1.password,
            ssh_pkey=self.ssh_node1.private_key,
            remote_bind_address=(self.ssh_node2.hostname, self.ssh_node2.port),
        )
        self.tunnel.start()

    def disconnect(self):
        if self.tunnel is not None:
            self.tunnel.stop()
            self.tunnel = None

        self.ssh_node1.disconnect()

    def execute_command(self, command, callback=None):
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                "127.0.0.1",
                self.tunnel.local_bind_port,
                username=self.ssh_node2.username,
            )
            result = self._execute_command(client.get_transport(), command)
            if callback:
                callback(result)
            return result

    def transfer_data(self, source_path, destination_path, callback=None):
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                "127.0.0.1",
                self.tunnel.local_bind_port,
                username=self.ssh_node2.username,
            )
            sftp_client = client.open_sftp()
            remote_file = self._upload_file(sftp_client, source_path, destination_path)
            sftp_client.close()
            if callback:
                callback(remote_file)
            return remote_file


if __name__ == "__main__":
    # 示例用法
    ssh_node1 = SSHNode("120.133.63.249", 22, "fengwen", "fengwen")
    ssh_node2 = SSHNode("192.168.1.27", 22, "fengwen", "fengwen")

    with ProxyNode(ssh_node1, ssh_node2) as proxy_node:
        # 执行命令
        output, error = proxy_node.execute_command("cd /home/fengwen/LLM_SPARK && ls")

        # 输出结果
        print("Output:", output)
        print("Error:", error)
        source_path = r"C:\Users\35348\Desktop\temp\project\main.py"
        destination_path = r"/home/fengwen/LLM_SPARK/main.py"
        proxy_node.transfer_data(source_path, destination_path)

    ssh_node1.connect()
