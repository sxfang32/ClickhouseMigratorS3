import clickhouse_connect
from typing import Optional

class CHClientManager:
    """ClickHouse客户端管理器"""
    
    def __init__(self):
        self.client = None
    
    def create_client(self, host: str, port: int, user: str, password: str) -> clickhouse_connect.driver.client.Client:
        """创建ClickHouse客户端连接"""
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                secure=False
            )
            # 验证连接
            client.query("SELECT 1")
            self.client = client
            return client
        except Exception as e:
            raise RuntimeError(f"ClickHouse连接失败：{str(e)}")
    
    def check_s3_policy(self, client: clickhouse_connect.driver.client.Client, s3_policy: str, logger) -> bool:
        """检查S3存储策略是否存在且可用"""
        import time
        test_table_name = f"__tmp_s3_policy_check_{int(time.time())}"
        try:
            # 步骤1：基础检查 - 验证存储策略名称是否存在
            policy_result = client.query(
                f"SELECT policy_name FROM system.storage_policies WHERE policy_name = '{s3_policy}'"
            )
            if not policy_result.result_rows:
                logger.error(f"S3存储策略{s3_policy}不存在！请检查策略名是否正确。")
                logger.info("查看所有可用存储策略：")
                all_policies = client.query("SELECT policy_name FROM system.storage_policies").result_rows
                logger.info(f"当前ClickHouse可用存储策略列表：{[p[0] for p in all_policies]}")
                return False

            # 步骤2：实际可用性验证 - 创建测试表验证策略是否能正常使用
            logger.info(f"开始验证存储策略{s3_policy}的可用性...")
            create_test_sql = f"""
            CREATE TABLE IF NOT EXISTS default.{test_table_name} (
                id UInt64,
                data String
            ) ENGINE = MergeTree()
            ORDER BY id
            SETTINGS storage_policy = '{s3_policy}'
            """
            # 执行创建测试表（验证策略是否可用）
            client.command(create_test_sql)

            # 步骤3：清理测试表
            client.command(f"DROP TABLE IF EXISTS default.{test_table_name}")

            logger.info(f"S3存储策略{s3_policy}验证通过（存在且可用）")
            return True

        except Exception as e:
            # 清理测试表（避免残留）
            try:
                client.command(f"DROP TABLE IF EXISTS default.{test_table_name}")
            except:
                pass

            logger.error(f"检查S3存储策略{s3_policy}失败：{str(e)}")
            # 给出针对性的错误建议
            if "storage_policy" in str(e):
                logger.error("可能原因：")
                logger.error("1. 存储策略名拼写错误；")
                logger.error("2. 存储策略配置错误（如S3密钥、桶名、endpoint配置错误）；")
                logger.error("3. ClickHouse服务无访问S3的网络权限；")
                logger.error("4. S3存储卷的磁盘配置错误。")
            return False
    
    def close(self):
        """关闭客户端连接"""
        if self.client:
            self.client.close()
