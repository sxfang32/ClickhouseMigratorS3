import argparse
import os
import yaml
from typing import Dict, Optional

DEFAULT_S3_POLICY = "s3"
DEFAULT_INSERT_INTERVAL = 1
DEFAULT_LOG_PATH = "./logs"
DEFAULT_REPORT_PATH = "./reports"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8123
DEFAULT_USER = "default"
DEFAULT_PASSWORD = ""

class ConfigManager:
    """配置管理器"""
    
    def __init__(self):
        self.config = {}
    
    def parse_args(self) -> argparse.Namespace:
        """解析命令行参数"""
        parser = argparse.ArgumentParser(
            description="ClickHouse表从本地存储策略迁移到S3存储策略脚本",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
                使用示例：
                1. 单表迁移：
                   python ch_s3_migration.py --mode single --db default --table test_table --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs
                2. 整库迁移：
                   python ch_s3_migration.py --mode full --db default --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs
                3. 断点续传迁移：
                   python ch_s3_migration.py --mode single --db default --table test_table --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs --resume
                    """
                )
        # 配置文件
        parser.add_argument("--config", help="配置文件路径")
        # 迁移模式
        parser.add_argument(
            "--mode",
            required=True,
            choices=["single", "full"],
            help="迁移模式：single（单表）/full（整库）"
        )
        # 数据库配置
        parser.add_argument("--db", required=True, help="目标数据库名")
        parser.add_argument("--table", help="单表迁移时指定表名")
        parser.add_argument("--host", default=DEFAULT_HOST, help="ClickHouse主机地址")
        parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="ClickHouse HTTP端口")
        parser.add_argument("--user", default=DEFAULT_USER, help="ClickHouse用户名")
        parser.add_argument("--password", default=DEFAULT_PASSWORD, help="ClickHouse密码")
        # S3策略配置
        parser.add_argument("--s3-policy", default=DEFAULT_S3_POLICY, help="S3存储策略名")
        # 迁移控制
        parser.add_argument("--insert-interval", type=float, default=DEFAULT_INSERT_INTERVAL,
                            help="分区插入间隔（秒），控制资源占用")
        parser.add_argument("--resume", action="store_true", help="启用断点续传")
        # 日志和报告
        parser.add_argument("--log-path", default=DEFAULT_LOG_PATH, help="日志存储路径")
        parser.add_argument("--report-path", default=DEFAULT_REPORT_PATH, help="迁移报告存储路径")

        args = parser.parse_args()

        # 参数校验
        if args.mode == "single" and not args.table:
            parser.error("单表迁移模式必须指定--table参数")

        # 创建日志和报告目录
        os.makedirs(args.log_path, exist_ok=True)
        os.makedirs(args.report_path, exist_ok=True)

        return args
    
    def load_config(self, config_path: Optional[str] = None) -> Dict:
        """加载配置文件"""
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        return {}
    
    def load_environment(self) -> Dict:
        """加载环境变量"""
        env_config = {
            "clickhouse": {
                "host": os.getenv("CH_HOST", DEFAULT_HOST),
                "port": int(os.getenv("CH_PORT", DEFAULT_PORT)),
                "user": os.getenv("CH_USER", DEFAULT_USER),
                "password": os.getenv("CH_PASSWORD", DEFAULT_PASSWORD)
            },
            "s3": {
                "policy": os.getenv("S3_POLICY", DEFAULT_S3_POLICY)
            },
            "migration": {
                "insert_interval": float(os.getenv("MIGRATION_INSERT_INTERVAL", DEFAULT_INSERT_INTERVAL)),
                "resume": os.getenv("MIGRATION_RESUME", "false").lower() == "true"
            },
            "logging": {
                "level": os.getenv("LOG_LEVEL", "info"),
                "path": os.getenv("LOG_PATH", DEFAULT_LOG_PATH)
            },
            "report": {
                "path": os.getenv("REPORT_PATH", DEFAULT_REPORT_PATH)
            }
        }
        return env_config
    
    def get_final_config(self, args: argparse.Namespace) -> Dict:
        """获取最终配置（优先级：命令行参数 > 环境变量 > 配置文件）"""
        # 加载配置文件
        config_file = self.load_config(args.config)
        # 加载环境变量
        env_config = self.load_environment()
        
        # 合并配置
        final_config = {
            "mode": args.mode,
            "db": args.db,
            "table": args.table,
            "host": args.host or env_config.get("clickhouse", {}).get("host", DEFAULT_HOST),
            "port": args.port or env_config.get("clickhouse", {}).get("port", DEFAULT_PORT),
            "user": args.user or env_config.get("clickhouse", {}).get("user", DEFAULT_USER),
            "password": args.password or env_config.get("clickhouse", {}).get("password", DEFAULT_PASSWORD),
            "s3_policy": args.s3_policy or env_config.get("s3", {}).get("policy", DEFAULT_S3_POLICY),
            "insert_interval": args.insert_interval or env_config.get("migration", {}).get("insert_interval", DEFAULT_INSERT_INTERVAL),
            "resume": args.resume or env_config.get("migration", {}).get("resume", False),
            "log_path": args.log_path or env_config.get("logging", {}).get("path", DEFAULT_LOG_PATH),
            "report_path": args.report_path or env_config.get("report", {}).get("path", DEFAULT_REPORT_PATH)
        }
        
        return final_config
