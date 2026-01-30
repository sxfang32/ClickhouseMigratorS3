# ClickHouse S3 迁移工具

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

## 项目介绍

ClickHouse S3 迁移工具是一个专门用于将 ClickHouse 表从本地存储策略迁移到 S3 存储策略的自动化工具。该工具具有以下特点：

- **支持两种迁移模式**：单表迁移和整库迁移
- **断点续传**：可在迁移中断后继续执行未完成的分区
- **数据一致性校验**：确保迁移前后数据完整性
- **完善的日志和报告**：详细记录迁移过程和结果
- **兼容多种分区类型**：支持单字段分区和复合分区
- **多配置方式**：支持命令行参数、配置文件和环境变量
- **分布式表支持**：自动识别和处理分布式表
- **现代化日志系统**：使用 loguru 提供更清晰、更强大的日志功能
- **进程锁机制**：防止并发迁移操作导致的数据不一致问题

## 安装方法

### 方法一：使用 pip 安装（开发模式）

```bash
cd ClickhouseMigratorS3
pip install -e .
```

### 方法二：直接运行

```bash
cd ClickhouseMigratorS3
pip install -r requirements.txt
python -m clickhouse_migrator.main --help
```

## 依赖项

- Python 3.7+
- clickhouse-connect >= 0.6.0
- PyYAML >= 6.0
- loguru >= 0.7.0

## 使用方法

### 命令行参数

```bash
clickhouse-migrator --help
```

### 单表迁移示例

```bash
clickhouse-migrator --mode single --db default --table test_table --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs
```

### 整库迁移示例

```bash
clickhouse-migrator --mode full --db default --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs
```

### 断点续传示例

```bash
clickhouse-migrator --mode single --db default --table test_table --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs --resume
```

### 分布式表迁移示例

```bash
# 分布式表的迁移与普通表相同，系统会自动识别并处理
clickhouse-migrator --mode single --db default --table distributed_table --host 127.0.0.1 --port 8123 --user default --password 123456 --s3-policy s3_policy --log-path ./logs
```

### 使用配置文件

创建 `config.yaml` 文件：

```yaml
clickhouse:
  host: 127.0.0.1
  port: 8123
  user: default
  password: ""

s3:
  policy: s3

migration:
  insert_interval: 1.0
  resume: false

logging:
  level: info
  path: ./logs

report:
  path: ./reports
```

然后运行：

```bash
clickhouse-migrator --config config.yaml --mode single --db default --table test_table
```

### 使用环境变量

```bash
export CH_HOST=127.0.0.1
export CH_PORT=8123
export CH_USER=default
export CH_PASSWORD=123456
export S3_POLICY=s3_policy
export LOG_PATH=./logs
clickhouse-migrator --mode single --db default --table test_table
```

## 配置选项

### 命令行参数

| 参数 | 说明 | 默认值 | 必需 |
|------|------|--------|------|
| `--mode` | 迁移模式：single（单表）/full（整库） | - | 是 |
| `--db` | 目标数据库名 | - | 是 |
| `--table` | 单表迁移时指定表名 | - | 单表模式必需 |
| `--host` | ClickHouse 主机地址 | 127.0.0.1 | 否 |
| `--port` | ClickHouse HTTP 端口 | 8123 | 否 |
| `--user` | ClickHouse 用户名 | default | 否 |
| `--password` | ClickHouse 密码 | "" | 否 |
| `--s3-policy` | S3 存储策略名 | s3 | 否 |
| `--insert-interval` | 分区插入间隔（秒） | 1.0 | 否 |
| `--resume` | 启用断点续传 | False | 否 |
| `--log-path` | 日志存储路径 | ./logs | 否 |
| `--report-path` | 迁移报告存储路径 | ./reports | 否 |
| `--config` | 配置文件路径 | - | 否 |

### 环境变量

| 环境变量 | 说明 | 默认值 |
|---------|------|-------|
| `CH_HOST` | ClickHouse 主机地址 | 127.0.0.1 |
| `CH_PORT` | ClickHouse HTTP 端口 | 8123 |
| `CH_USER` | ClickHouse 用户名 | default |
| `CH_PASSWORD` | ClickHouse 密码 | "" |
| `S3_POLICY` | S3 存储策略名 | s3 |
| `MIGRATION_INSERT_INTERVAL` | 分区插入间隔（秒） | 1.0 |
| `MIGRATION_RESUME` | 启用断点续传 | false |
| `LOG_LEVEL` | 日志级别 | info |
| `LOG_PATH` | 日志存储路径 | ./logs |
| `REPORT_PATH` | 迁移报告存储路径 | ./reports |

## 迁移流程

1. **环境检查**：检查 S3 存储策略是否存在且可用
2. **创建备份表**：基于源表结构创建使用 S3 存储策略的备份表
3. **分区迁移**：
   - 逐个分区将数据从源表插入备份表
   - 校验每个分区的数据一致性
   - 删除源表中的分区数据
   - 更新迁移进度
4. **全表校验**：确认所有数据已正确迁移
5. **表替换**：删除源表，将备份表重命名为源表名
6. **生成报告**：生成详细的迁移报告

## 迁移报告

迁移完成后，工具会在 `--report-path` 指定的目录生成 JSON 格式的迁移报告，包含以下信息：

- 迁移基本信息（模式、数据库、表等）
- 每个表的迁移结果（开始时间、结束时间、状态等）
- 分区级别的详细信息（行数、校验结果等）
- 整体迁移统计（成功/失败/跳过的表数）

## 注意事项

1. **数据安全**：迁移过程中会删除源表的分区数据，建议在迁移前进行数据备份
2. **性能影响**：迁移过程会占用一定的系统资源，建议在业务低峰期执行
3. **网络依赖**：依赖 S3 存储服务的可用性和网络稳定性
4. **存储策略配置**：需确保 S3 存储策略配置正确，包括密钥、桶名、endpoint 等
5. **权限要求**：执行脚本的用户需要有足够的 ClickHouse 操作权限
6. **分布式表注意事项**：
   - 分布式表迁移会自动处理其关联的本地表
   - 需要确保所有节点都能访问 S3 存储服务
   - 迁移过程可能需要更长时间，因为需要处理多个本地表

7. **进程锁机制注意事项**：
   - 工具会在 `./locks` 目录创建锁文件，确保同一时间只有一个进程迁移特定表
   - 锁文件命名格式为 `{db}_{table}.lock`，包含进程 ID 和获取时间信息
   - 如果迁移过程意外终止，锁文件可能会残留，工具会自动清理无效锁文件
   - 锁获取超时时间为 3600 秒，可根据实际情况调整

## 错误处理

### 常见错误及解决方法

| 错误类型 | 可能原因 | 解决方法 |
|---------|---------|---------|
| S3 策略不存在 | 存储策略名拼写错误 | 检查策略名，使用 `SHOW STORAGE POLICIES` 查看可用策略 |
| S3 连接失败 | 网络问题或密钥配置错误 | 检查网络连接和 S3 密钥配置 |
| 表不存在 | 表名拼写错误或权限不足 | 检查表名和用户权限 |
| 分区键错误 | 表未配置分区键 | 为表添加分区键或使用其他迁移方法 |
| 数据校验失败 | 数据传输过程中出错 | 检查网络稳定性，使用断点续传重新迁移 |

## 故障恢复

如果迁移过程中出现错误，可以：

1. **检查备份表**：备份表 `{table}_backup_s3` 中可能已经包含了部分或全部数据
2. **使用断点续传**：添加 `--resume` 参数重新运行，工具会跳过已完成的分区
3. **手动恢复**：如果数据损坏严重，可以从 ClickHouse 备份中恢复源表

## 系统架构

项目采用模块化设计，主要包含以下组件：

- **命令行接口**：解析命令行参数，启动迁移流程
- **配置管理器**：管理系统配置，支持多种配置方式
- **迁移协调器**：协调各个服务的执行，管理迁移流程
- **ClickHouse 客户端**：管理与 ClickHouse 的连接和操作
- **迁移服务**：执行具体的迁移逻辑
- **分区管理器**：管理表的分区信息
- **数据验证器**：验证数据一致性
- **断点续传服务**：管理迁移进度，支持断点续传
- **报告服务**：生成迁移报告
- **日志管理器**：管理系统日志
- **进程锁管理器**：管理表迁移的进程锁，防止并发操作

详细架构设计请参考 [ARCHITECTURE.md](ARCHITECTURE.md) 文件。

