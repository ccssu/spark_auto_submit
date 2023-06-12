import os
import zipfile
from tqdm import tqdm

__all__ = ["DefaultZipManager"]


class DefaultZipManager:
    def __init__(self, directory_path):
        self.directory_path = directory_path

    def get_dir_files(self):
        # 每个文件绝对路径
        files_list = []
        for root, dirs, files in os.walk(self.directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                files_list.append(file_path)
        return files_list

    def _compress_files(self, files, zip_file, progress_bar, arcname=None):
        progress_bar.total = len(files)
        for file_path in files:
            # arcname: 压缩文件中的文件名
            if arcname is None:
                arcname = os.path.dirname(self.directory_path)
            arcname = os.path.relpath(file_path, arcname)
            zip_file.write(file_path, arcname=arcname)
            progress_bar.update(1)

    def zip_code_files(self, file_list, output_path, description="Compressing"):
        zip_path = output_path
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = tqdm(unit="file(s)", desc=description)

            self._compress_files(file_list, zip_file, progress_bar, self.directory_path)
            progress_bar.close()
        return zip_path

    def zip_files(self, file_list, output_path, description="Compressing"):
        zip_path = output_path
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = tqdm(unit="file(s)", desc=description)
            self._compress_files(file_list, zip_file, progress_bar)
            progress_bar.close()
        return zip_path

    def zip_directory(
        self, directory_path, output_path=None, description="Compressing"
    ):

        # 实现 zip -r xxx.zip xxx
        self.directory_path = directory_path
        file_list = self.get_dir_files()
        return self.zip_files(file_list, output_path, description)


if __name__ == "__main__":
    # 示例用法
    directory_path = "/opt/conda/envs/spark_tool"
    zip_manager = ZipManager(directory_path)
    with working_directory(directory_path):
        zip_path = zip_manager.zip_files(directory_path, "spark_tool_env.zip")
        print("Zip file created:", zip_path)
    # 压缩当前目录下的所有文件
