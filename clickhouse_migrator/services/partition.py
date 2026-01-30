import re
from typing import List, Optional

class PartitionManager:
    """分区管理器"""
    
    def get_table_partition_key(self, client, db: str, table: str) -> str:
        """
        从system.tables获取表的分区键表达式（兼容单/复合分区）
        返回示例：单分区→'idate'，复合分区→'dt, channel'
        """
        try:
            result = client.query(f"""
                SELECT partition_key 
                FROM system.tables 
                WHERE database = '{db}' AND name = '{table}'
            """)
            if not result.result_rows or not result.result_rows[0][0]:
                raise RuntimeError(f"表{db}.{table}未配置分区键（PARTITION BY），无法按分区迁移")

            # 清理分区键格式：移除外层括号，如 (idate) → idate，(dt, channel) → dt, channel
            partition_key = result.result_rows[0][0].strip()
            if partition_key.startswith('(') and partition_key.endswith(')'):
                partition_key = partition_key[1:-1].strip()
            return partition_key
        except Exception as e:
            raise RuntimeError(f"获取{db}.{table}分区键失败：{str(e)}")
    
    def generate_partition_where_clause(self, partition_key: str, partition_value: str) -> str:
        """
        根据分区键和分区值生成适配的WHERE条件（核心优化）
        :param partition_key: 分区键（如'idate'、'dt, channel'）
        :param partition_value: 分区值（如'2024-01-01'、'(\'2024-01-01\',\'novel\')'）
        :return: WHERE条件，示例：
                 单分区 → "idate = '2024-01-01'"
                 复合分区 → "(dt = '2024-01-01') AND (channel = 'novel')"
        """
        # 清理分区值：移除外层括号（如复合分区值 ('2024-01-01','novel') → '2024-01-01','novel'）
        partition_value = partition_value.strip()
        if partition_value.startswith('(') and partition_value.endswith(')'):
            partition_value = partition_value[1:-1].strip()

        # 智能分割分区值（兼容带引号的字符串，避免按逗号错误分割）
        # 正则匹配规则：匹配带引号的字符串（如'2024-01-01'）或非逗号的字符序列（如12345）
        value_parts = re.findall(r"'[^']*'|[^,]+", partition_value)
        value_parts = [v.strip() for v in value_parts if v.strip()]

        # 分割分区键为单个字段
        key_parts = [k.strip() for k in partition_key.split(',') if k.strip()]

        # 校验分区键和值的数量匹配（避免复合分区不匹配）
        if len(key_parts) != len(value_parts):
            raise RuntimeError(
                f"分区键与值数量不匹配！\n"
                f"分区键：{partition_key}（共{len(key_parts)}个字段）\n"
                f"分区值：{partition_value}（共{len(value_parts)}个值）"
            )

        # 生成每个字段的条件（自动处理引号/数值类型）
        condition_parts = []
        for key, value in zip(key_parts, value_parts):
            # 数值类型（int/float）：直接匹配，不加引号
            if not (value.startswith("'") and value.endswith("'")):
                try:
                    float(value)  # 尝试转换为数值，验证是否为数值类型
                    condition_parts.append(f"{key} = {value}")
                except ValueError:
                    # 字符串/日期类型：添加引号
                    condition_parts.append(f"{key} = '{value}'")
            else:
                # 已带引号的字符串（如'2024-01-01'）：直接使用
                condition_parts.append(f"{key} = {value}")

        # 复合分区用AND连接，每个条件加括号保证优先级
        return " AND ".join([f"({cond})" for cond in condition_parts])
    
    def format_partition_value_for_drop(self, partition_value: str) -> str:
        """
        为DROP PARTITION语句格式化分区值（核心修复）
        规则：
        - 数值类型（如20240101）→ 直接返回，不加引号；
        - 字符串/日期类型（如2023-12-31）→ 加单引号；
        - 复合分区（如('2024-01-01','novel')）→ 直接返回（已有正确格式）；
        """
        # 先清理空格
        partition_value = partition_value.strip()

        # 情况1：复合分区（包含括号）→ 直接返回
        if partition_value.startswith('(') and partition_value.endswith(')'):
            return partition_value

        # 情况2：判断是否为纯数值（整数/浮点数）→ 不加引号
        try:
            # 尝试转换为数值，验证是否为数值类型
            float(partition_value)
            return partition_value
        except ValueError:
            # 情况3：字符串/日期类型 → 加单引号
            # 避免重复加引号（如分区值已带引号的情况）
            if not (partition_value.startswith("'") and partition_value.endswith("'")):
                return f"'{partition_value}'"
            return partition_value
    
    def get_table_partitions(self, client, db: str, table: str) -> List[str]:
        """获取表的所有有效分区值列表（兼容单/复合分区）"""
        try:
            result = client.query(
                f"""
                SELECT DISTINCT partition 
                FROM system.parts 
                WHERE database = '{db}' AND table = '{table}' AND active = 1 
                ORDER BY partition
                """
            )
            partitions = [row[0] for row in result.result_rows]
            return partitions
        except Exception as e:
            raise RuntimeError(f"获取{db}.{table}分区列表失败：{str(e)}")
