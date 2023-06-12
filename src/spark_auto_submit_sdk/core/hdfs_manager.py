__all__ = ["HdfsManager"]


class HdfsManager:
    def __init__(
        self, ssh_client, hdfs_url, hadoop_home=r"/usr/local/service/hadoop/bin/hadoop"
    ):
        self.ssh_client = ssh_client
        self.hdfs_url = hdfs_url
        self.hadoop_home = hadoop_home

    def execute_hadoop_cmd(self, cmd):
        full_cmd = f"{self.hadoop_home} fs {cmd}"
        stdin, stdout, stderr = self.ssh_client.exec_command(full_cmd)
        return (
            stdout.channel.recv_exit_status(),
            stdout.read().decode().strip(),
            stderr.read().decode().strip(),
        )

    def ls(self, path):
        cmd = f"-ls {self.hdfs_url}/{path}"
        return self.execute_hadoop_cmd(cmd)

    def add_file(self, local_path, hdfs_path):
        cmd = f"-put {local_path} {self.hdfs_url}/{hdfs_path}"
        return self.execute_hadoop_cmd(cmd)

    def delete_file(self, hdfs_path):
        cmd = f"-rm {self.hdfs_url}/{hdfs_path}"
        return self.execute_hadoop_cmd(cmd)

    def read_file(self, hdfs_path):
        cmd = f"-cat {self.hdfs_url}/{hdfs_path}"
        return self.execute_hadoop_cmd(cmd)

    def write_file(self, local_path, hdfs_path):
        cmd = f"-copyFromLocal {local_path} {self.hdfs_url}/{hdfs_path}"
        return self.execute_hadoop_cmd(cmd)

    def close(self):
        self.ssh_client.close()


if __name__ == "__main__":
    import paramiko

    # Create an SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect("remote_host", username="username", password="password")

    # Create an instance of HdfsManager with the SSH client
    manager = HdfsManager(ssh_client, "hdfs://10.1.0.22:4007")

    # Perform remote HDFS operations using the manager object
    return_code, stdout, stderr = manager.ls("fengwen/test_demo/data")
    if return_code == 0:
        print(stdout)
    else:
        print(f"Error: {stderr}")

    return_code, stdout, stderr = manager.add_file(
        "/path/to/local/file.txt", "fengwen/test_demo/data/file.txt"
    )
    if return_code == 0:
        print("File added successfully")
    else:
        print(f"Error: {stderr}")

    return_code, stdout, stderr = manager.delete_file("fengwen/test_demo/data/file.txt")
    if return_code == 0:
        print("File deleted successfully")
    else:
        print(f"Error: {stderr}")

    return_code, stdout, stderr = manager.read_file("fengwen/test_demo/data/file.txt")
    if return_code == 0:
        print(stdout)
    else:
        print(f"Error: {stderr}")

    return_code, stdout, stderr = manager.write_file(
        "/path/to/local/file.txt", "fengwen/test_demo/data/file.txt"
    )
    if return_code == 0:
        print("File written successfully")
    else:
        print(f"Error: {stderr}")

    # Close the SSH connection
    manager.close()
