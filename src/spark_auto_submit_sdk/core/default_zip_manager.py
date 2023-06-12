import os
import zipfile
from pathlib import Path
from tqdm import tqdm

__all__ = ["DefaultZipManager"]



class DefaultZipManager:
    def __init__(self, directory_path: str):
        self.directory_path = Path(directory_path)

    def get_dir_files(self) -> list:
        # 获取目录下的文件列表
        return list(self.directory_path.glob('**/*'))

    def _compress_files(self, files: list, zip_file: zipfile.ZipFile, progress_bar: tqdm, arcname: str):
        progress_bar.total = len(files)
        for file_path in files:
            file_path = Path(file_path) if isinstance(file_path, str) else file_path
            rel_path = file_path.relative_to(arcname)
            zip_file.write(file_path, arcname=rel_path)
            progress_bar.update(1)

    def zip_files(self, file_list: list, output_path: str, description: str = "Compressing",*,arcname=None) -> str:
        zip_path = Path(output_path)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = tqdm(unit="file(s)", desc=description)
            arcname = Path(self.directory_path) if arcname is None else arcname
            self._compress_files(file_list, zip_file, progress_bar, arcname=arcname)
            progress_bar.close()
        return str(zip_path)

    def zip_directory(self, directory_path: str, output_path: str = None, description: str = "Compressing") -> str:
        directory_path = Path(directory_path)
        file_list = self.get_dir_files()
        if output_path is None:
            output_path = directory_path.with_suffix('.zip')
        return self.zip_files(file_list, output_path,description = description, arcname = Path(self.directory_path).parent)





if __name__ == "__main__":
    # 示例用法
    # 创建 DefaultZipManager 实例
    zip_manager = DefaultZipManager('/workspace/DEMO/test_project00/pyspark_venv')

    # 获取目录下的所有文件
    file_list = zip_manager.get_dir_files()
    print(file_list)
    # 压缩文件列表为 ZIP 文件
    output_path = '/workspace/DEMO/test_project00/pyspark_venv.zip'
    # zip_manager.zip_files(file_list, output_path, description='Compressing files')

    # # 压缩整个目录为 ZIP 文件
    directory_path = '/workspace/DEMO/test_project00/pyspark_venv'
    # output_path = '/path/to/output.zip'
    zip_manager.zip_directory(directory_path, output_path, description='Compressing directory')

