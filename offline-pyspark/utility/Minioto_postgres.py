import json
import logging

import ujson
from tqdm import tqdm
from minio import Minio, S3Error

import psycopg2
from typing import  List, Dict, Any

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MinIO 配置
minio_endpoint = "cdh03:9003"
minio_access_key = "root"
minio_secret_key = "12345678"
minio_secure = False

# PostgreSQL 配置
pg_host = "cdh03"
pg_port = 5432
pg_dbname = "aaa"
pg_user = "postgres"
pg_password = "root"
pg_table = "user_device_base"  # 目标表名

# 初始化 MinIO 客户端
minio_client = Minio(
    endpoint=minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=minio_secure
)


def collect_minio_user_device_2postgresql(bucket_name: str) -> int:
    """
    从 MinIO 读取数据并解析，准备写入 PostgreSQL
    :param bucket_name: MinIO 桶名
    :return: 成功解析的记录数，失败返回 0
    """
    all_records: List[Dict[str, Any]] = []  # 存储所有解析后的记录
    total_bytes_processed: int = 0  # 已处理的字节数
    total_file_size: int = 0  # 所有文件的总大小
    batch_size: int = 1000  # 批量写入阈值（可根据实际调整）

    # 检查桶是否存在
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        logger.error(f"Bucket {bucket_name} 不存在！")
        return 0

    # 预计算总文件大小（用于进度条）
    objects = list(minio_client.list_objects(bucket_name, recursive=True))
    for obj in objects:
        total_file_size += obj.size

    # 处理每个文件
    with tqdm(total=total_file_size, desc="读取数据", unit="B", unit_scale=True) as pbar:
        for obj in objects:
            file_name = obj.object_name
            file_size = obj.size
            logger.info(f"开始处理文件: {file_name}, 大小: {file_size / (1024 * 1024):.2f} MB")

            # 读取 MinIO 对象
            try:
                response = minio_client.get_object(bucket_name, file_name)
            except S3Error as e:
                logger.error(f"读取文件 {file_name} 失败: {e}")
                continue

            temp_buffer = ""
            try:
                # 逐块读取
                for chunk in response.stream(8192):
                    chunk_size = len(chunk)
                    total_bytes_processed += chunk_size
                    pbar.update(chunk_size)

                    # 拼接缓冲区（处理可能的断行）
                    temp_buffer += chunk.decode('utf-8', errors='ignore')

                    # 按行解析
                    while '\n' in temp_buffer:
                        line, temp_buffer = temp_buffer.split('\n', 1)
                        try:
                            res_json = ujson.loads(line.strip())
                            # 提取需要的字段（避免 KeyError，可根据实际 JSON 结构调整默认值）
                            record = {
                                "brand": res_json.get('brand', None),
                                "plat": res_json.get('plat', None),
                                "platv": res_json.get('platv', None),
                                "softv": res_json.get('softv', None),
                                "uname": res_json.get('uname', None),
                                "userkey": res_json.get('userkey', None),
                                "datatype": res_json.get('datatype', None),
                                "device": res_json.get('device', None),
                                "ip": res_json.get('ip', None),
                                "net": res_json.get('net', None),
                                "opa": res_json.get('opa', None)
                            }
                            all_records.append(record)

                            # 批量写入（达到阈值时执行）
                            if len(all_records) >= batch_size:
                                write_to_postgresql(all_records)
                                all_records.clear()

                        except json.JSONDecodeError as e:
                            logger.warning(f"解析 JSON 失败: {e}, 行内容: {line[:100]}...")
                        except Exception as e:
                            logger.warning(f"处理行数据失败: {e}, 行内容: {line[:100]}...")

            finally:
                response.close()
                response.release_conn()
                logger.info(f"文件 {file_name} 处理完成")

    # 处理剩余数据
    if all_records:
        write_to_postgresql(all_records)

    return len(all_records)


def write_to_postgresql(records: List[Dict[str, Any]]) -> None:
    """
    批量写入 PostgreSQL
    :param records: 待写入的记录列表
    """
    if not records:
        return

    # SQL 插入语句（根据表结构调整字段）
    columns = ', '.join(records[0].keys())
    placeholders = ', '.join(['%s'] * len(records[0]))
    sql = f"INSERT INTO {pg_table} ({columns}) VALUES ({placeholders})"

    try:
        # 建立连接
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_dbname,
            user=pg_user,
            password=pg_password
        )
        cursor = conn.cursor()

        # 构造参数（按字段顺序提取值）
        data = [tuple(record.values()) for record in records]

        # 执行批量插入
        cursor.executemany(sql, data)
        conn.commit()
        logger.info(f"成功写入 {len(records)} 条记录到 PostgreSQL")

    except psycopg2.Error as e:
        logger.error(f"写入 PostgreSQL 失败: {e}")
        conn.rollback()  # 回滚事务
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    bucket_name = "aaa"  # 替换为 MinIO 中真实存在的桶名
    result = collect_minio_user_device_2postgresql(bucket_name)
    logger.info(f"任务完成，共处理 {result} 条记录")