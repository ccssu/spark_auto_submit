from setuptools import setup, find_packages

setup(
    name="spark_auto_submit_sdk",
    version="v1.0.0",
    description="Enterprise-level Spark Auto Submit SDK",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        # 列出项目所需的所有依赖项
        "Fabric",
        "gitignore_parser",
        "paramiko",
        "PyYAML",
        "setuptools",
        "sshtunnel",
        "tqdm",
    ],
    entry_points={
        "console_scripts": ["spark_auto_submit_sdk=spark_auto_submit_sdk.__main__:main"]
    },
)
