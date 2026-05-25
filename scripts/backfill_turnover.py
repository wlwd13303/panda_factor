"""
独立脚本：补全 factor_base 表中 turnover_f（换手率/自由流通股）字段
只更新指定日期区间内已有的记录，不重新跑全量清洗任务
"""
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from pymongo import UpdateOne

from panda_common.config import get_config
from panda_common.logger_config import logger
from panda_common.handlers.database_handler import DatabaseHandler
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client
from panda_data_hub.utils.ts_utils import get_tushare_suffix


def backfill_turnover_f(start_date: str, end_date: str):
    """
    补全指定日期区间的 turnover_f 字段

    Args:
        start_date: 起始日期，格式 'YYYYMMDD' 或 'YYYY-MM-DD'
        end_date: 结束日期，格式 'YYYYMMDD' 或 'YYYY-MM-DD'
    """
    config = get_config()
    db_handler = DatabaseHandler(config)
    init_tushare_client(config)
    pro = get_tushare_client()

    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_list = [d.strftime('%Y%m%d') for d in date_range]

    total_dates = len(date_list)
    total_updated = 0

    logger.info(f"开始补全 turnover_f，日期范围: {start_date} ~ {end_date}，共 {total_dates} 天")

    for i, date in enumerate(date_list):
        # 检查当天 factor_base 中是否有数据
        records = db_handler.mongo_find(
            config["MONGO_DB"], 'factor_base',
            {"date": date, "turnover": {"$exists": False}}
        )

        if not records:
            logger.info(f"[{i+1}/{total_dates}] {date}: 无需更新（无记录或已全部补全）")
            continue

        symbols = [r['symbol'] for r in records]
        logger.info(f"[{i+1}/{total_dates}] {date}: {len(symbols)} 条记录待补全")

        try:
            factor_data = pro.query(
                'daily_basic', trade_date=date,
                fields=['ts_code', 'turnover_rate']
            )
            if factor_data is None or factor_data.empty:
                logger.warning(f"  {date}: tushare 无数据")
                time.sleep(0.3)
                continue

            # 构建 symbol -> turnover_rate 的映射
            factor_data['symbol'] = factor_data['ts_code']
            turnover_map = dict(zip(factor_data['symbol'], factor_data['turnover_rate']))

            upsert_operations = []
            for record in records:
                symbol = record['symbol']
                turnover_f_val = turnover_map.get(symbol)
                if turnover_f_val is not None:
                    upsert_operations.append(UpdateOne(
                        {'_id': record['_id']},
                        {'$set': {'turnover': turnover_f_val}}
                    ))

            if upsert_operations:
                result = db_handler.mongo_client[config["MONGO_DB"]]['factor_base'].bulk_write(
                    upsert_operations
                )
                updated = result.modified_count
                total_updated += updated
                logger.info(f"  {date}: 成功更新 {updated}/{len(symbols)} 条")
            else:
                logger.info(f"  {date}: 未匹配到 turnover 数据")

        except Exception as e:
            logger.error(f"  {date}: 处理失败 - {str(e)}")

        time.sleep(0.5)  # 避免 tushare API 限流

    logger.info(f"补全完成！共更新 {total_updated} 条记录")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='补全 factor_base 表的 turnover_f 字段')
    parser.add_argument('start_date', help='起始日期，如 20240101')
    parser.add_argument('end_date', help='结束日期，如 20241231')
    args = parser.parse_args()

    backfill_turnover_f(args.start_date, args.end_date)
