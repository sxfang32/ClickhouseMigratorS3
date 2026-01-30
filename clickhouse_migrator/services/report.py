import json
import os
from datetime import datetime
from typing import List, Dict

REPORT_PREFIX = "clickhouse_s3_migration_report"

class ReportService:
    """报告服务"""
    
    def generate_migration_report(self, config: Dict, migration_results: List[Dict], logger) -> str:
        """
        生成迁移报告
        :return: 报告文件路径
        """
        report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(config["report_path"], f"{REPORT_PREFIX}_{report_time}.json")

        # 计算详细统计信息，包括分布式表的本地表
        total_tables = len(migration_results)
        completed_tables = len([r for r in migration_results if r["status"] == "completed"])
        failed_tables = len([r for r in migration_results if r["status"] == "failed"])
        skipped_tables = len([r for r in migration_results if r["status"] == "skipped"])
        
        # 计算本地表统计信息
        total_local_tables = 0
        completed_local_tables = 0
        failed_local_tables = 0
        
        for result in migration_results:
            if "local_tables" in result:
                local_tables = result["local_tables"]
                total_local_tables += len(local_tables)
                completed_local_tables += len([lt for lt in local_tables if lt["status"] == "completed"])
                failed_local_tables += len([lt for lt in local_tables if lt["status"] == "failed"])

        report = {
            "migration_info": {
                "mode": config["mode"],
                "database": config["db"],
                "table": config["table"] if config["mode"] == "single" else "all",
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "s3_policy": config["s3_policy"],
                "clickhouse_config": {
                    "host": config["host"],
                    "port": config["port"],
                    "user": config["user"]
                }
            },
            "results": migration_results,
            "summary": {
                "total_tables": total_tables,
                "completed_tables": completed_tables,
                "failed_tables": failed_tables,
                "skipped_tables": skipped_tables,
                "distributed_tables": {
                    "total_local_tables": total_local_tables,
                    "completed_local_tables": completed_local_tables,
                    "failed_local_tables": failed_local_tables
                }
            }
        }

        # 保存报告
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"迁移报告已生成：{report_file}")
        # 控制台输出汇总信息
        logger.info("=" * 50)
        logger.info("迁移汇总：")
        logger.info(f"总表数：{total_tables}")
        logger.info(f"成功：{completed_tables}")
        logger.info(f"失败：{failed_tables}")
        logger.info(f"跳过：{skipped_tables}")
        if total_local_tables > 0:
            logger.info(f"分布式表本地表统计：")
            logger.info(f"  总本地表数：{total_local_tables}")
            logger.info(f"  成功：{completed_local_tables}")
            logger.info(f"  失败：{failed_local_tables}")
        logger.info("=" * 50)
        
        return report_file
    
    def log_migration_summary(self, migration_results: List[Dict], logger):
        """
        记录迁移摘要
        """
        completed = len([r for r in migration_results if r["status"] == "completed"])
        failed = len([r for r in migration_results if r["status"] == "failed"])
        skipped = len([r for r in migration_results if r["status"] == "skipped"])
        
        # 计算本地表统计信息
        total_local_tables = 0
        completed_local_tables = 0
        failed_local_tables = 0
        
        for result in migration_results:
            if "local_tables" in result:
                local_tables = result["local_tables"]
                total_local_tables += len(local_tables)
                completed_local_tables += len([lt for lt in local_tables if lt["status"] == "completed"])
                failed_local_tables += len([lt for lt in local_tables if lt["status"] == "failed"])
        
        logger.info("=" * 50)
        logger.info("迁移完成摘要：")
        logger.info(f"总表数：{len(migration_results)}")
        logger.info(f"成功完成：{completed}")
        logger.info(f"失败：{failed}")
        logger.info(f"跳过：{skipped}")
        
        if total_local_tables > 0:
            logger.info(f"分布式表本地表统计：")
            logger.info(f"  总本地表数：{total_local_tables}")
            logger.info(f"  成功：{completed_local_tables}")
            logger.info(f"  失败：{failed_local_tables}")
        
        logger.info("=" * 50)
        
        total_failed = failed + failed_local_tables
        if total_failed > 0:
            logger.error(f"有{total_failed}个表迁移失败，请查看日志和报告获取详细信息")
        else:
            logger.info("所有表迁移成功完成！")
