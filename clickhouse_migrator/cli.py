import sys
from clickhouse_migrator.config import ConfigManager
from clickhouse_migrator.orchestrator import MigrationOrchestrator

def main():
    """主入口函数"""
    # 解析命令行参数
    config_manager = ConfigManager()
    args = config_manager.parse_args()
    
    # 获取最终配置
    config = config_manager.get_final_config(args)
    
    # 创建迁移协调器
    orchestrator = MigrationOrchestrator()
    
    # 执行迁移
    migration_results, success = orchestrator.orchestrate_migration(config)
    
    # 退出状态
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
