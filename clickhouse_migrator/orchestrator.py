import os
from typing import List, Dict
from loguru import logger

class MigrationOrchestrator:
    """迁移协调器"""
    
    def __init__(self):
        from clickhouse_migrator.clients.ch_client import CHClientManager
        from clickhouse_migrator.services.migration import MigrationService
        from clickhouse_migrator.services.report import ReportService
        from clickhouse_migrator.services.resume import ResumeService
        from clickhouse_migrator.utils.logging import setup_logger
        
        self.ch_client_manager = CHClientManager()
        self.migration_service = MigrationService()
        self.report_service = ReportService()
        self.resume_service = ResumeService()
        self.setup_logger = setup_logger
    
    def orchestrate_migration(self, config: Dict):
        """
        协调迁移流程
        :param config: 配置字典
        :return: 迁移结果列表
        """
        # 初始化日志
        self.setup_logger(config["log_path"])
        logger.info("=" * 50)
        logger.info("开始ClickHouse表迁移到S3存储策略")
        logger.info(f"迁移模式：{config['mode']}，目标数据库：{config['db']}")
        if config["mode"] == "single":
            logger.info(f"目标表：{config['table']}")
        logger.info("=" * 50)

        try:
            # 1. 创建ClickHouse客户端
            client = self.ch_client_manager.create_client(
                config["host"],
                config["port"],
                config["user"],
                config["password"]
            )
            logger.info("ClickHouse连接成功")

            # 2. 环境检查
            if not self.ch_client_manager.check_s3_policy(client, config["s3_policy"], logger):
                raise RuntimeError("S3存储策略检查失败，终止迁移")

            # 3. 加载断点续传进度
            progress = self.resume_service.load_migration_progress()
            logger.info(f"断点续传状态：{'启用' if config['resume'] else '禁用'}")
            if config["resume"] and os.path.exists("migration_progress.json"):
                logger.info(f"加载迁移进度文件：migration_progress.json")

            # 4. 执行迁移
            migration_results = []
            if config["mode"] == "single":
                # 单表迁移
                result = self.migration_service.migrate_single_table(
                    client, config, logger, progress, config["db"], config["table"]
                )
                migration_results.append(result)
            else:
                # 整库迁移
                migration_results = self.migration_service.migrate_full_database(
                    client, config, logger, progress
                )

            # 5. 生成迁移报告
            self.report_service.generate_migration_report(config, migration_results, logger)

            # 6. 最终状态检查
            failed_tables = [r for r in migration_results if r["status"] == "failed"]
            if failed_tables:
                logger.error(f"迁移完成，但有{len(failed_tables)}个表迁移失败")
                return migration_results, False
            else:
                logger.info("所有表迁移成功完成！")
                return migration_results, True

        except Exception as e:
            import traceback
            error_msg = f"迁移流程异常终止：{str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            return [], False
        finally:
            # 关闭客户端连接
            self.ch_client_manager.close()
