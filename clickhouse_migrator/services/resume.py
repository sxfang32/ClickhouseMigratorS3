import json
import os
from typing import Dict, List

PROGRESS_FILE = "migration_progress.json"

class ResumeService:
    """断点续传服务"""
    
    def load_migration_progress(self) -> Dict:
        """加载迁移进度文件"""
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    
    def save_migration_progress(self, progress: Dict):
        """保存迁移进度文件"""
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def get_uncompleted_partitions(
            self,
            progress: Dict,
            db: str,
            table: str,
            all_partitions: List[str]
    ) -> List[str]:
        """获取未完成的分区列表（断点续传）"""
        if db not in progress or table not in progress[db]:
            return all_partitions

        table_progress = progress[db][table]
        if table_progress["status"] == "completed":
            return []

        completed_partitions = table_progress.get("completed_partitions", [])
        uncompleted = [p for p in all_partitions if p not in completed_partitions]
        return uncompleted
    
    def initialize_table_progress(self, progress: Dict, db: str, table: str) -> Dict:
        """初始化表级进度"""
        if db not in progress:
            progress[db] = {}
        if table not in progress[db]:
            progress[db][table] = {
                "completed_partitions": [],
                "status": "running"
            }
        return progress
    
    def update_partition_progress(self, progress: Dict, db: str, table: str, partition: str):
        """更新分区进度"""
        if db in progress and table in progress[db]:
            if partition not in progress[db][table]["completed_partitions"]:
                progress[db][table]["completed_partitions"].append(partition)
            self.save_migration_progress(progress)
    
    def mark_table_completed(self, progress: Dict, db: str, table: str):
        """标记表迁移完成"""
        if db in progress and table in progress[db]:
            progress[db][table]["status"] = "completed"
            self.save_migration_progress(progress)
    
    def mark_table_failed(self, progress: Dict, db: str, table: str):
        """标记表迁移失败"""
        if db in progress and table in progress[db]:
            progress[db][table]["status"] = "failed"
            self.save_migration_progress(progress)
