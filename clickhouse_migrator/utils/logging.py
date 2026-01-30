import os
from datetime import datetime
from loguru import logger

def setup_logger(log_path: str, log_level: str = "info"):
    """初始化日志配置（控制台+文件双输出）"""
    # 清除默认的logger
    logger.remove()
    
    # 控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=""),
        format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
        level="INFO"
    )
    
    # 文件输出
    log_file = os.path.join(log_path, f"ch_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logger.add(
        sink=log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} - {name} - {level} - {file}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )
    
    return logger
