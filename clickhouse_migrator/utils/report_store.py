import os
from datetime import datetime
from typing import Dict

REPORT_PREFIX = "clickhouse_s3_migration_report"

def generate_report_path(report_dir: str) -> str:
    """
    生成报告文件路径
    :param report_dir: 报告存储目录
    :return: 报告文件完整路径
    """
    report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(report_dir, f"{REPORT_PREFIX}_{report_time}.json")

def ensure_report_dir(report_dir: str):
    """
    确保报告目录存在
    :param report_dir: 报告存储目录
    """
    os.makedirs(report_dir, exist_ok=True)

def get_report_directory() -> str:
    """
    获取默认报告目录
    :return: 默认报告目录路径
    """
    return os.path.abspath("./reports")
