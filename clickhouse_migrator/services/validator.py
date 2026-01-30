from typing import Optional

class DataValidator:
    """数据验证器"""
    
    def get_row_count(self,
            client,
            db: str,
            table: str,
            partition_value: Optional[str] = None,
            partition_key: Optional[str] = None
    ) -> int:
        """
        获取表/分区的行数（兼容动态分区键）
        :param partition_value: 分区值（如'2024-01-01'）
        :param partition_key: 表的分区键（如'idate'）
        """
        try:
            if partition_value and partition_key:
                # 生成动态WHERE条件
                from clickhouse_migrator.services.partition import PartitionManager
                partition_manager = PartitionManager()
                where_clause = partition_manager.generate_partition_where_clause(partition_key, partition_value)
                query = f"""
                SELECT count(*) 
                FROM {db}.{table} 
                WHERE {where_clause}
                """
            else:
                # 全表计数
                query = f"SELECT count(*) FROM {db}.{table}"

            result = client.query(query)
            return int(result.result_rows[0][0])
        except Exception as e:
            raise RuntimeError(f"获取{db}.{table}行数失败（分区：{partition_value}）：{str(e)}")
    
    def validate_partition(self, client, db: str, src_table: str, dst_table: str, 
                          partition_value: str, partition_key: str) -> dict:
        """
        验证分区数据一致性
        :return: 校验结果字典
        """
        try:
            src_count = self.get_row_count(client, db, src_table, partition_value, partition_key)
            dst_count = self.get_row_count(client, db, dst_table, partition_value, partition_key)
            
            result = {
                "partition": partition_value,
                "src_count": src_count,
                "dst_count": dst_count,
                "passed": src_count == dst_count
            }
            return result
        except Exception as e:
            raise RuntimeError(f"验证分区{partition_value}数据一致性失败：{str(e)}")
    
    def validate_table(self, client, db: str, src_table: str, dst_table: str) -> dict:
        """
        验证全表数据一致性
        :return: 校验结果字典
        """
        try:
            src_count = self.get_row_count(client, db, src_table)
            dst_count = self.get_row_count(client, db, dst_table)
            
            result = {
                "src_count": src_count,
                "dst_count": dst_count,
                "passed": src_count == dst_count
            }
            return result
        except Exception as e:
            raise RuntimeError(f"验证表{db}.{src_table}数据一致性失败：{str(e)}")
