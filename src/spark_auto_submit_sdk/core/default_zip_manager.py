import os
import zipfile
from tqdm import tqdm

__all__ = ["DefaultZipManager"]


class DefaultZipManager:
    def __init__(self, directory_path):
        self.directory_path = directory_path

    def get_dir_files(self):
        files_list = []
        for root, dirs, files in os.walk(self.directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, self.directory_path)
                files_list.append(relative_path)
        return files_list

    def _compress_files(self, files, zip_file, progress_bar):
        progress_bar.total = len(files)
        for file in files:
            zip_file.write(file)
            progress_bar.update(1)

    def zip_files(
        self, file_list, save_to_dir, zip_filename, description="Compressing"
    ):
        zip_path = os.path.join(save_to_dir, zip_filename)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = tqdm(unit="file(s)", desc=description)
            self._compress_files(file_list, zip_file, progress_bar)
            progress_bar.close()
        return zip_path


if __name__ == "__main__":
    # 示例用法
    directory_path = "/opt/conda/envs/spark_tool"
    zip_manager = ZipManager(directory_path)
    with working_directory(directory_path):
        zip_path = zip_manager.zip_files(directory_path, "spark_tool_env.zip")
        print("Zip file created:", zip_path)
    # 压缩当前目录下的所有文件
