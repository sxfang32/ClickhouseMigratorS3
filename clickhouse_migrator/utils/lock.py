import os
import time
import fcntl
from typing import Optional

class TableLock:
    """表迁移锁管理"""
    
    def __init__(self, lock_dir: str = "./locks"):
        """
        初始化锁管理器
        :param lock_dir: 锁文件存储目录
        """
        self.lock_dir = lock_dir
        os.makedirs(self.lock_dir, exist_ok=True)
    
    def get_lock_file(self, db: str, table: str) -> str:
        """
        获取表的锁文件路径
        :param db: 数据库名
        :param table: 表名
        :return: 锁文件路径
        """
        lock_file = f"{db}_{table}.lock"
        return os.path.join(self.lock_dir, lock_file)
    
    def acquire_lock(self, db: str, table: str, timeout: int = 3600) -> Optional[object]:
        """
        获取表迁移锁
        :param db: 数据库名
        :param table: 表名
        :param timeout: 超时时间（秒）
        :return: 锁文件对象，如果获取失败返回None
        """
        lock_file_path = self.get_lock_file(db, table)
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # 尝试打开锁文件
                lock_file = open(lock_file_path, 'w')
                # 尝试获取排他锁
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # 写入进程信息
                lock_file.write(f"pid: {os.getpid()}\n")
                lock_file.write(f"time: {time.time()}\n")
                lock_file.flush()
                return lock_file
            except BlockingIOError:
                # 锁被其他进程占用
                time.sleep(5)  # 等待5秒后重试
            except Exception as e:
                # 其他错误
                return None
        
        # 超时
        return None
    
    def release_lock(self, lock_file: object):
        """
        释放表迁移锁
        :param lock_file: 锁文件对象
        """
        if lock_file:
            try:
                # 释放锁
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                # 关闭文件
                lock_file.close()
                # 删除锁文件
                os.unlink(lock_file.name)
            except Exception:
                # 忽略错误，确保即使出现异常也能继续执行
                pass
    
    def is_locked(self, db: str, table: str) -> bool:
        """
        检查表是否被锁定
        :param db: 数据库名
        :param table: 表名
        :return: 如果被锁定返回True，否则返回False
        """
        lock_file_path = self.get_lock_file(db, table)
        if not os.path.exists(lock_file_path):
            return False
        
        try:
            lock_file = open(lock_file_path, 'r')
            # 尝试获取非阻塞锁
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            # 能够获取锁，说明锁文件存在但没有进程持有
            os.unlink(lock_file_path)  # 清理无效锁文件
            return False
        except BlockingIOError:
            # 无法获取锁，说明有进程持有
            return True
        except Exception:
            # 其他错误，假设未锁定
            return False
