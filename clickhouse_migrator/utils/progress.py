import json
import os
from typing import Dict

PROGRESS_FILE = "migration_progress.json"

def load_progress() -> Dict:
    """加载迁移进度文件"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_progress(progress: Dict):
    """保存迁移进度文件"""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

def get_progress_file_path() -> str:
    """获取进度文件路径"""
    return os.path.abspath(PROGRESS_FILE)
