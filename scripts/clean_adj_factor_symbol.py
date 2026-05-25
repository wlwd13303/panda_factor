"""
删除 adj_factor 集合中 symbol 为旧格式（纯数字，无 .SH/.SZ/.BJ 后缀）的脏数据
分批删除，避免超时
用法: python scripts/clean_adj_factor_symbol.py [--dry-run] [--batch 10000]
"""

import sys
import os
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from panda_common.config import get_config
from panda_common.handlers.database_handler import DatabaseHandler

config = get_config()
db_handler = DatabaseHandler(config)
collection = db_handler.get_mongo_collection(config["MONGO_DB"], "adj_factor")

dry_run = "--dry-run" in sys.argv
batch_size = int(next((a.split("=")[1] for a in sys.argv if a.startswith("--batch=")), 10000))
filter_query = {"symbol": {"$not": {"$regex": r"^\d{6}\.(SH|SZ|BJ)$"}}}

total_count = collection.count_documents(filter_query)
print(f"发现 {total_count} 条脏数据（symbol 不含交易所后缀）")

if dry_run:
    print(f"[DRY RUN] 不会实际删除，将按每批 {batch_size} 条模拟")
    print("去掉 --dry-run 参数执行真正删除")
elif total_count == 0:
    print("没有需要清理的数据")
else:
    deleted_total = 0
    while True:
        ids = [doc["_id"] for doc in collection.find(filter_query, {"_id": 1}).limit(batch_size)]
        if not ids:
            break
        result = collection.delete_many({"_id": {"$in": ids}})
        deleted_total += result.deleted_count
        print(f"  已删除 {deleted_total} / {total_count} 条")

    print(f"清理完成，共删除 {deleted_total} 条记录")

db_handler.mongo_client.close()
