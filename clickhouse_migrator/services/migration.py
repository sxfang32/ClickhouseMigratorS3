import re
import time
import traceback
from datetime import datetime
from typing import List, Dict, Optional

class MigrationService:
    """迁移服务"""
    
    def __init__(self):
        from clickhouse_migrator.services.partition import PartitionManager
        from clickhouse_migrator.services.validator import DataValidator
        from clickhouse_migrator.services.resume import ResumeService
        from clickhouse_migrator.utils.lock import TableLock
        
        self.partition_manager = PartitionManager()
        self.validator = DataValidator()
        self.resume_service = ResumeService()
        self.table_lock = TableLock()
    
    def get_create_table_sql(self, client, db: str, table: str, logger) -> str:
        """获取表的完整建表语句（兼容不同ClickHouse版本的返回格式）"""
        try:
            # 执行SHOW CREATE TABLE，兼容带/不带FORMAT的写法
            query = f"SHOW CREATE TABLE {db}.{table}"
            result = client.query(query)

            # 校验返回结果是否为空
            if not result.result_rows:
                raise RuntimeError(f"执行{query}未返回任何结果，表可能不存在")

            # 兼容不同版本的返回格式：
            # 版本1：返回两列（表名, 建表语句）→ 取索引1
            # 版本2：返回一列（建表语句）→ 取索引0
            first_row = result.result_rows[0]
            if len(first_row) >= 2:
                create_sql = first_row[1]
            else:
                create_sql = first_row[0]

            # 清理建表语句中的多余空格/换行，确保格式正确
            create_sql = " ".join(create_sql.split())
            logger.debug(f"获取到{db}.{table}的建表语句：{create_sql}")
            return create_sql
        except Exception as e:
            raise RuntimeError(f"获取{db}.{table}建表语句失败：{str(e)}")
    
    def modify_create_sql_for_s3(self, create_sql: str, s3_policy: str, table: str, backup_suffix: str = "_backup_s3") -> str:
        """修改建表语句，替换为S3存储策略，并生成备份表建表语句"""
        # 1. 生成备份表名（保留原数据库）
        backup_table = table + backup_suffix

        # 关键修改1：忽略大小写匹配 CREATE TABLE [db.]table，精准捕获库名和表名
        # 匹配模式：兼容 create table / CREATE TABLE，支持空格/换行，捕获 (库名, 表名)
        pattern = re.compile(r"(CREATE\s+TABLE\s+)([^\s.]+)\.([^\s]+)", re.IGNORECASE)
        match = pattern.search(create_sql)
        if match:
            # 捕获到库名（如dws）和原表名（如ads_book_panel_30di）
            db_name = match.group(2)
            original_table = match.group(3)
            # 替换为 库名.备份表名
            create_sql = pattern.sub(
                f"\\1{db_name}.{backup_table}",
                create_sql,
                count=1
            )
        else:
            # 兼容无库名的情况（如CREATE TABLE table (...)）
            pattern_no_db = re.compile(r"(CREATE\s+TABLE\s+)([^\s]+)", re.IGNORECASE)
            create_sql = pattern_no_db.sub(
                f"\\1{backup_table}",
                create_sql,
                count=1
            )

        # 2. 处理storage_policy：确保添加到原SETTINGS后，兼容大小写
        # 情况1：已有storage_policy → 替换（忽略大小写）
        storage_policy_pattern = re.compile(r"storage_policy\s*=\s*['\"][^'\"]+['\"]", re.IGNORECASE)
        if storage_policy_pattern.search(create_sql):
            create_sql = storage_policy_pattern.sub(
                f"storage_policy = '{s3_policy}'",
                create_sql
            )
        # 情况2：有SETTINGS但无storage_policy → 追加（忽略大小写）
        elif re.search(r"SETTINGS\s+", create_sql, re.IGNORECASE):
            create_sql = re.sub(
                r"(SETTINGS\s+[^;]+)",  # 匹配SETTINGS后的内容
                r"\1, storage_policy = '" + s3_policy + "'",
                create_sql,
                flags=re.IGNORECASE
            )
        # 情况3：无SETTINGS → 在ENGINE后添加
        else:
            # 匹配ENGINE行末尾（兼容MergeTree/其他引擎，忽略大小写）
            create_sql = re.sub(
                r"(ENGINE\s*=\s*MergeTree\s*[^;]+)(;?)",
                r"\1 SETTINGS storage_policy = '" + s3_policy + r"'\2",
                create_sql,
                flags=re.IGNORECASE
            )

        # 清理多余空格，确保格式正确
        create_sql = re.sub(r"\s+", " ", create_sql)
        return create_sql
    
    def migrate_single_table(self, client, config: Dict, logger, progress: Dict, db: str, table: str) -> Dict:
        """迁移单个表到S3存储策略（兼容任意分区字段/复合分区）"""
        migration_result = {
            "table": table,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": "",
            "status": "failed",
            "total_partitions": 0,
            "completed_partitions": 0,
            "total_rows": 0,
            "migrated_rows": 0,
            "error": "",
            "check_results": []
        }

        backup_table = table + "_backup_s3"
        try:
            # 1. 检查源表是否存在且为本地存储策略
            logger.info(f"开始迁移表：{db}.{table}")
            create_sql = self.get_create_table_sql(client, db, table, logger)
            if config["s3_policy"] in create_sql:
                logger.warning(f"{db}.{table}已使用S3存储策略，跳过迁移")
                migration_result["status"] = "skipped"
                return migration_result

            # 2. 创建备份表（S3存储策略）
            new_create_sql = self.modify_create_sql_for_s3(create_sql, config["s3_policy"], table)
            logger.debug(f"备份表建表语句：{new_create_sql}")
            client.command(f"DROP TABLE IF EXISTS {db}.{backup_table}")
            client.command(new_create_sql)

            # 校验备份表是否存在
            def check_table_exists(client, db, table, logger):
                result = client.query(f"SELECT name FROM system.tables WHERE database = '{db}' AND name = '{table}'")
                return len(result.result_rows) > 0

            if not check_table_exists(client, db, backup_table, logger):
                raise RuntimeError(f"备份表{db}.{backup_table}创建失败！建表语句：\n{new_create_sql}")
            logger.info(f"创建备份表成功：{db}.{backup_table}")

            # 3. 获取分区列表+动态解析分区键
            all_partitions = self.partition_manager.get_table_partitions(client, db, table)
            if not all_partitions:
                logger.warning(f"{db}.{table}无分区数据，直接重命名")
                client.command(f"DROP TABLE {db}.{table}")
                client.command(f"RENAME TABLE {db}.{backup_table} TO {db}.{table}")
                migration_result["status"] = "completed"
                migration_result["total_partitions"] = 0
                migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return migration_result

            # 解析表的实际分区键（如idate、dt、date_dt、复合分区）
            partition_key = self.partition_manager.get_table_partition_key(client, db, table)
            logger.info(f"表{db}.{table}的分区键：{partition_key}")

            migration_result["total_partitions"] = len(all_partitions)
            uncompleted_partitions = self.resume_service.get_uncompleted_partitions(progress, db, table, all_partitions)
            if uncompleted_partitions:
                logger.info(f"待迁移分区数：{len(uncompleted_partitions)}，分区列表：{uncompleted_partitions}")
            else:
                logger.info(f"{db}.{table}所有分区已迁移完成")
                migration_result["status"] = "completed"
                migration_result["completed_partitions"] = len(all_partitions)
                migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return migration_result

            # 4. 初始化表级进度
            progress = self.resume_service.initialize_table_progress(progress, db, table)

            # 5. 全表总行数统计
            total_rows = self.validator.get_row_count(client, db, table)
            migration_result["total_rows"] = total_rows
            logger.info(f"{db}.{table}总数据量：{total_rows}行")

            # 6. 逐个分区迁移（兼容任意分区字段）
            for idx, partition in enumerate(uncompleted_partitions):
                logger.info(f"开始迁移分区：[{idx + 1}/{len(uncompleted_partitions)}]：{partition}")
                start_time = time.time()

                # 6.1 生成动态WHERE条件，插入分区数据
                where_clause = self.partition_manager.generate_partition_where_clause(partition_key, partition)
                insert_sql = f"""
                INSERT INTO {db}.{backup_table} 
                SELECT * FROM {db}.{table} WHERE {where_clause}
                """
                client.command(insert_sql)
                time.sleep(config["insert_interval"])  # 资源控制

                # 6.2 分区数据一致性校验
                src_count = self.validator.get_row_count(client, db, table, partition, partition_key)
                dst_count = self.validator.get_row_count(client, db, backup_table, partition, partition_key)
                check_result = {
                    "partition": partition,
                    "src_count": src_count,
                    "dst_count": dst_count,
                    "passed": src_count == dst_count,
                    "cost_time": round(time.time() - start_time, 2)
                }
                migration_result["check_results"].append(check_result)

                if not check_result["passed"]:
                    raise RuntimeError(
                        f"分区{partition}数据校验失败：源表{src_count}行，备份表{dst_count}行"
                    )
                logger.info(f"分区{partition}校验通过，原始条数：{check_result['src_count']}，迁移条数：{check_result['dst_count']}，耗时{check_result['cost_time']}秒")

                # 6.3 删除源表当前分区数据（核心修复：格式化分区值）
                formatted_partition = self.partition_manager.format_partition_value_for_drop(partition)
                drop_partition_sql = f"ALTER TABLE {db}.{table} DROP PARTITION {formatted_partition}"
                logger.debug(f"删除分区SQL：{drop_partition_sql}")
                client.command(drop_partition_sql)
                logger.info(f"源表分区{partition}数据已删除\n")

                # 6.4 更新进度
                self.resume_service.update_partition_progress(progress, db, table, partition)
                migration_result["completed_partitions"] += 1
                migration_result["migrated_rows"] += src_count

            # 7. 全表数据一致性校验
            logger.info("开始全表数据校验")
            src_total = self.validator.get_row_count(client, db, table)  # 理论上应为0
            dst_total = self.validator.get_row_count(client, db, backup_table)
            if src_total != 0 or dst_total != total_rows:
                raise RuntimeError(
                    f"全表校验失败：源表剩余{src_total}行，备份表{dst_total}行（预期{total_rows}行）"
                )
            logger.info(f"全表数据校验通过，原表行数：{total_rows}，迁移后行数：{dst_total}")

            # 8. 重命名表（最终替换）
            logger.info("开始替换源表")
            client.command(f"DROP TABLE IF EXISTS {db}.{table}")
            client.command(f"RENAME TABLE {db}.{backup_table} TO {db}.{table}")
            logger.info(f"表{db}.{table}迁移完成，已切换到S3存储策略")

            # 9. 更新迁移结果
            migration_result["status"] = "completed"
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.resume_service.mark_table_completed(progress, db, table)

        except Exception as e:
            error_msg = f"迁移表{db}.{table}失败：{str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            migration_result["error"] = error_msg
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.resume_service.mark_table_failed(progress, db, table)
            # 恢复建议
            logger.warning(
                f"恢复建议：1. 检查备份表{db}.{backup_table}数据完整性；2. 修复错误后使用--resume参数续传；3. 若数据损坏，从ClickHouse备份恢复源表"
            )
        finally:
            # 释放迁移锁
            if lock_file:
                self.table_lock.release_lock(lock_file)
                logger.info(f"释放表{db}.{table}迁移锁成功")

        return migration_result
    
    def is_distributed_table(self, client, db: str, table: str) -> bool:
        """判断表是否为分布式表"""
        try:
            result = client.query(f"""
                SELECT engine 
                FROM system.tables 
                WHERE database = '{db}' AND name = '{table}'
            """)
            if result.result_rows:
                return result.result_rows[0][0] == 'Distributed'
            return False
        except Exception as e:
            raise RuntimeError(f"判断表{db}.{table}是否为分布式表失败：{str(e)}")
    
    def get_local_tables(self, client, db: str, distributed_table: str) -> List[Dict]:
        """获取分布式表关联的本地表信息"""
        try:
            result = client.query(f"""
                SELECT engine_full 
                FROM system.tables 
                WHERE database = '{db}' AND name = '{distributed_table}'
            """)
            if not result.result_rows:
                raise RuntimeError(f"获取分布式表{db}.{distributed_table}的引擎信息失败")
            
            engine_full = result.result_rows[0][0]
            # 解析 engine_full 获取本地表信息
            # 格式: Distributed('cluster', 'database', 'table', sharding_key)
            pattern = r"Distributed\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]"
            match = re.search(pattern, engine_full)
            if not match:
                raise RuntimeError(f"无法解析分布式表引擎信息：{engine_full}")
            
            cluster = match.group(1)
            local_db = match.group(2)
            local_table = match.group(3)
            
            return [{
                "cluster": cluster,
                "db": local_db,
                "table": local_table
            }]
        except Exception as e:
            raise RuntimeError(f"获取分布式表{db}.{distributed_table}的本地表信息失败：{str(e)}")
    
    def migrate_distributed_table(self, client, config: Dict, logger, progress: Dict, db: str, table: str) -> Dict:
        """迁移分布式表"""
        migration_result = {
            "table": table,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": "",
            "status": "failed",
            "error": "",
            "local_tables": []
        }
        
        try:
            logger.info(f"开始迁移分布式表：{db}.{table}")
            
            # 1. 获取本地表信息
            local_tables = self.get_local_tables(client, db, table)
            logger.info(f"分布式表{db}.{table}关联的本地表：{local_tables}")
            
            # 2. 迁移本地表
            local_results = []
            for local_table_info in local_tables:
                logger.info(f"迁移本地表：{local_table_info['db']}.{local_table_info['table']}")
                # 检查本地表是否被锁定
                if self.table_lock.is_locked(local_table_info['db'], local_table_info['table']):
                    logger.warning(f"本地表{local_table_info['db']}.{local_table_info['table']}正在被其他进程迁移，跳过迁移")
                    local_result = {
                        "table": local_table_info['table'],
                        "status": "locked",
                        "error": "表正在被其他进程迁移"
                    }
                    local_results.append(local_result)
                    raise RuntimeError(f"本地表{local_table_info['db']}.{local_table_info['table']}正在被其他进程迁移")
                
                local_result = self.migrate_single_table(
                    client, config, logger, progress, 
                    local_table_info['db'], local_table_info['table']
                )
                local_results.append(local_result)
                
                if local_result["status"] == "failed":
                    logger.error(f"本地表{local_table_info['db']}.{local_table_info['table']}迁移失败")
                    raise RuntimeError(f"本地表迁移失败：{local_result['error']}")
                elif local_result["status"] == "locked" or local_result["status"] == "lock_failed":
                    logger.error(f"本地表{local_table_info['db']}.{local_table_info['table']}锁操作失败")
                    raise RuntimeError(f"本地表锁操作失败：{local_result['status']}")
            
            # 3. 重建分布式表（如果需要）
            logger.info(f"分布式表{db}.{table}的本地表迁移完成")
            # 注意：分布式表本身不需要存储数据，只需要确保本地表迁移完成
            
            # 4. 更新迁移结果
            migration_result["status"] = "completed"
            migration_result["local_tables"] = local_results
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"分布式表{db}.{table}迁移完成")
            
        except Exception as e:
            error_msg = f"迁移分布式表{db}.{table}失败：{str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            migration_result["error"] = error_msg
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return migration_result
    
    def migrate_single_table(self, client, config: Dict, logger, progress: Dict, db: str, table: str) -> Dict:
        """迁移单个表到S3存储策略（兼容任意分区字段/复合分区）"""
        # 检查是否为分布式表
        if self.is_distributed_table(client, db, table):
            return self.migrate_distributed_table(client, config, logger, progress, db, table)
        
        migration_result = {
            "table": table,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": "",
            "status": "failed",
            "total_partitions": 0,
            "completed_partitions": 0,
            "total_rows": 0,
            "migrated_rows": 0,
            "error": "",
            "check_results": []
        }

        backup_table = table + "_backup_s3"
        lock_file = None
        try:
            # 1. 检查表是否被锁定
            if self.table_lock.is_locked(db, table):
                logger.warning(f"表{db}.{table}正在被其他进程迁移，跳过迁移")
                migration_result["status"] = "locked"
                return migration_result
            
            # 2. 获取迁移锁
            lock_file = self.table_lock.acquire_lock(db, table)
            if not lock_file:
                logger.error(f"获取表{db}.{table}迁移锁失败，跳过迁移")
                migration_result["status"] = "lock_failed"
                return migration_result
            
            logger.info(f"获取表{db}.{table}迁移锁成功")
            
            # 3. 检查源表是否存在且为本地存储策略
            logger.info(f"开始迁移表：{db}.{table}")
            create_sql = self.get_create_table_sql(client, db, table, logger)
            if config["s3_policy"] in create_sql:
                logger.warning(f"{db}.{table}已使用S3存储策略，跳过迁移")
                migration_result["status"] = "skipped"
                return migration_result

            # 2. 创建备份表（S3存储策略）
            new_create_sql = self.modify_create_sql_for_s3(create_sql, config["s3_policy"], table)
            logger.debug(f"备份表建表语句：{new_create_sql}")
            client.command(f"DROP TABLE IF EXISTS {db}.{backup_table}")
            client.command(new_create_sql)

            # 校验备份表是否存在
            def check_table_exists(client, db, table, logger):
                result = client.query(f"SELECT name FROM system.tables WHERE database = '{db}' AND name = '{table}'")
                return len(result.result_rows) > 0

            if not check_table_exists(client, db, backup_table, logger):
                raise RuntimeError(f"备份表{db}.{backup_table}创建失败！建表语句：\n{new_create_sql}")
            logger.info(f"创建备份表成功：{db}.{backup_table}")

            # 3. 获取分区列表+动态解析分区键
            all_partitions = self.partition_manager.get_table_partitions(client, db, table)
            if not all_partitions:
                logger.warning(f"{db}.{table}无分区数据，直接重命名")
                client.command(f"DROP TABLE {db}.{table}")
                client.command(f"RENAME TABLE {db}.{backup_table} TO {db}.{table}")
                migration_result["status"] = "completed"
                migration_result["total_partitions"] = 0
                migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return migration_result

            # 解析表的实际分区键（如idate、dt、date_dt、复合分区）
            partition_key = self.partition_manager.get_table_partition_key(client, db, table)
            logger.info(f"表{db}.{table}的分区键：{partition_key}")

            migration_result["total_partitions"] = len(all_partitions)
            uncompleted_partitions = self.resume_service.get_uncompleted_partitions(progress, db, table, all_partitions)
            if uncompleted_partitions:
                logger.info(f"待迁移分区数：{len(uncompleted_partitions)}，分区列表：{uncompleted_partitions}")
            else:
                logger.info(f"{db}.{table}所有分区已迁移完成")
                migration_result["status"] = "completed"
                migration_result["completed_partitions"] = len(all_partitions)
                migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return migration_result

            # 4. 初始化表级进度
            progress = self.resume_service.initialize_table_progress(progress, db, table)

            # 5. 全表总行数统计
            total_rows = self.validator.get_row_count(client, db, table)
            migration_result["total_rows"] = total_rows
            logger.info(f"{db}.{table}总数据量：{total_rows}行")

            # 6. 逐个分区迁移（兼容任意分区字段）
            for idx, partition in enumerate(uncompleted_partitions):
                logger.info(f"开始迁移分区：[{idx + 1}/{len(uncompleted_partitions)}]：{partition}")
                start_time = time.time()

                # 6.1 生成动态WHERE条件，插入分区数据
                where_clause = self.partition_manager.generate_partition_where_clause(partition_key, partition)
                insert_sql = f"""
                INSERT INTO {db}.{backup_table} 
                SELECT * FROM {db}.{table} WHERE {where_clause}
                """
                client.command(insert_sql)
                time.sleep(config["insert_interval"])

                # 6.2 分区数据一致性校验
                src_count = self.validator.get_row_count(client, db, table, partition, partition_key)
                dst_count = self.validator.get_row_count(client, db, backup_table, partition, partition_key)
                check_result = {
                    "partition": partition,
                    "src_count": src_count,
                    "dst_count": dst_count,
                    "passed": src_count == dst_count,
                    "cost_time": round(time.time() - start_time, 2)
                }
                migration_result["check_results"].append(check_result)

                if not check_result["passed"]:
                    raise RuntimeError(
                        f"分区{partition}数据校验失败：源表{src_count}行，备份表{dst_count}行"
                    )
                logger.info(f"分区{partition}校验通过，原始条数：{check_result['src_count']}，迁移条数：{check_result['dst_count']}，耗时{check_result['cost_time']}秒")

                # 6.3 删除源表当前分区数据（核心修复：格式化分区值）
                formatted_partition = self.partition_manager.format_partition_value_for_drop(partition)
                drop_partition_sql = f"ALTER TABLE {db}.{table} DROP PARTITION {formatted_partition}"
                logger.debug(f"删除分区SQL：{drop_partition_sql}")
                client.command(drop_partition_sql)
                logger.info(f"源表分区{partition}数据已删除\n")

                # 6.4 更新进度
                self.resume_service.update_partition_progress(progress, db, table, partition)
                migration_result["completed_partitions"] += 1
                migration_result["migrated_rows"] += src_count

            # 7. 全表数据一致性校验
            logger.info("开始全表数据校验")
            src_total = self.validator.get_row_count(client, db, table)
            dst_total = self.validator.get_row_count(client, db, backup_table)
            if src_total != 0 or dst_total != total_rows:
                raise RuntimeError(
                    f"全表校验失败：源表剩余{src_total}行，备份表{dst_total}行（预期{total_rows}行）"
                )
            logger.info(f"全表数据校验通过，原表行数：{total_rows}，迁移后行数：{dst_total}")

            # 8. 重命名表（最终替换）
            logger.info("开始替换源表")
            client.command(f"DROP TABLE IF EXISTS {db}.{table}")
            client.command(f"RENAME TABLE {db}.{backup_table} TO {db}.{table}")
            logger.info(f"表{db}.{table}迁移完成，已切换到S3存储策略")

            # 9. 更新迁移结果
            migration_result["status"] = "completed"
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.resume_service.mark_table_completed(progress, db, table)

        except Exception as e:
            error_msg = f"迁移表{db}.{table}失败：{str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            migration_result["error"] = error_msg
            migration_result["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.resume_service.mark_table_failed(progress, db, table)
            # 恢复建议
            logger.warning(
                f"恢复建议：1. 检查备份表{db}.{backup_table}数据完整性；2. 修复错误后使用--resume参数续传；3. 若数据损坏，从ClickHouse备份恢复源表"
            )

        return migration_result
    
    def migrate_full_database(self, client, config: Dict, logger, progress: Dict) -> List[Dict]:
        """整库迁移：迁移指定数据库下所有本地存储策略的表"""
        logger.info(f"开始整库迁移：{config['db']}")
        # 获取数据库下所有表
        tables_result = client.query(
            f"SELECT name FROM system.tables WHERE database = '{config['db']}' AND engine NOT IN ('View', 'MaterializedView')"
        )
        tables = [row[0] for row in tables_result.result_rows]
        logger.info(f"发现{config['db']}数据库下可迁移表数量：{len(tables)}")

        # 逐个迁移表
        migration_results = []
        for table in tables:
            result = self.migrate_single_table(client, config, logger, progress, config['db'], table)
            migration_results.append(result)
            # 表迁移失败时是否继续（可根据需求调整）
            if result["status"] == "failed":
                logger.warning(f"表{table}迁移失败，继续处理下一个表")

        return migration_results
