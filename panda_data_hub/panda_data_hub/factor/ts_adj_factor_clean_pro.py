import time
import traceback
from datetime import datetime, timedelta

import pandas as pd
from pymongo import UpdateOne

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client
from panda_data_hub.utils.ts_utils import get_tushare_suffix


class TSAdjFactorCleaner:

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None

        init_tushare_client(config)
        self.pro = get_tushare_client()

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def _report_progress(self, progress_info: dict):
        if self.progress_callback:
            self.progress_callback(progress_info)

    def clean_daily_adj_factor(self):
        """清洗当日复权因子（供定时任务调用）"""
        try:
            date = datetime.now().strftime('%Y%m%d')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"No stock_market records found for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol']]
            data['ts_code'] = data['symbol'].apply(get_tushare_suffix)

            logger.info("正在获取复权因子数据......")
            adj_factor_data = self.pro.adj_factor(trade_date=date, fields='ts_code,adj_factor')
            if adj_factor_data is None or adj_factor_data.empty:
                logger.warning(f"No adj_factor data returned from Tushare for {date}")
                return

            result_data = data.merge(adj_factor_data[['ts_code', 'adj_factor']], on='ts_code', how='left')
            result_data['symbol'] = result_data['ts_code']
            result_data = result_data.drop(columns=['ts_code'])

            ensure_collection_and_indexes(table_name='adj_factor')

            upsert_operations = []
            for record in result_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))

            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['adj_factor'].bulk_write(upsert_operations)
                logger.info(f"Successfully upserted {len(upsert_operations)} adj_factor records for date: {date}")

        except Exception as e:
            error_msg = f"Failed to process adj_factor data for date {date}: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def clean_history_adj_factor(self, start_date: str, end_date: str, force_update: bool = False):
        """清洗历史复权因子数据（按交易日遍历全市场）"""
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date)

        self._report_progress({
            "progress_percent": 0,
            "status": "running",
            "current_task": "正在获取交易日列表...",
            "trading_days_processed": 0,
            "trading_days_total": 0,
            "current_date": "",
            "stock_processed": 0,
            "stock_total": 0,
            "error_message": ""
        })

        # 获取 stock_market 中的交易日列表
        db = self.db_handler.mongo_client[self.config["MONGO_DB"]]
        trading_days = sorted(db["stock_market"].distinct(
            "date",
            {"date": {"$gte": start_date, "$lte": end_date}}
        ))

        if not trading_days:
            self._report_progress({
                "progress_percent": 100,
                "status": "completed",
                "current_task": "指定的日期范围内没有行情数据",
                "trading_days_processed": 0,
                "trading_days_total": 0
            })
            return

        total_days = len(trading_days)
        ensure_collection_and_indexes(table_name='adj_factor')

        adj_collection = db["adj_factor"]

        for day_idx, trade_date in enumerate(trading_days):
            percent = int((day_idx / total_days) * 100)

            # 检查是否已有数据
            if not force_update:
                existing_count = adj_collection.count_documents({"date": trade_date})
                if existing_count > 0:
                    self._report_progress({
                        "progress_percent": percent,
                        "status": "running",
                        "current_task": f"跳过已有数据的交易日: {trade_date}",
                        "trading_days_processed": day_idx + 1,
                        "trading_days_total": total_days,
                        "current_date": trade_date,
                        "stock_processed": existing_count,
                        "stock_total": existing_count,
                        "error_message": ""
                    })
                    continue

            self._report_progress({
                "progress_percent": percent,
                "status": "running",
                "current_task": f"正在获取 {trade_date} 的复权因子...",
                "trading_days_processed": day_idx,
                "trading_days_total": total_days,
                "current_date": trade_date,
                "stock_processed": 0,
                "stock_total": 0,
                "error_message": ""
            })

            try:
                # 获取该交易日所有股票的复权因子
                adj_factor_data = self.pro.adj_factor(
                    trade_date=trade_date,
                    fields='ts_code,adj_factor'
                )

                if adj_factor_data is None or adj_factor_data.empty:
                    self._report_progress({
                        "progress_percent": int(((day_idx + 1) / total_days) * 100),
                        "status": "running",
                        "current_task": f"{trade_date} 无复权因子数据返回",
                        "trading_days_processed": day_idx + 1,
                        "trading_days_total": total_days,
                        "current_date": trade_date,
                        "stock_processed": 0,
                        "stock_total": 0
                    })
                    continue

                # 保留 Tushare 原始 ts_code 格式作为 symbol（如 600519.SH）
                adj_factor_data['symbol'] = adj_factor_data['ts_code']
                adj_factor_data['date'] = trade_date
                adj_factor_data = adj_factor_data[['date', 'symbol', 'adj_factor']]

                upsert_operations = []
                for record in adj_factor_data.to_dict('records'):
                    upsert_operations.append(UpdateOne(
                        {'date': record['date'], 'symbol': record['symbol']},
                        {'$set': record},
                        upsert=True
                    ))

                if upsert_operations:
                    adj_collection.bulk_write(upsert_operations)

                self._report_progress({
                    "progress_percent": int(((day_idx + 1) / total_days) * 100),
                    "status": "running",
                    "current_task": f"完成 {trade_date}，写入 {len(upsert_operations)} 条",
                    "trading_days_processed": day_idx + 1,
                    "trading_days_total": total_days,
                    "current_date": trade_date,
                    "stock_processed": len(upsert_operations),
                    "stock_total": len(upsert_operations),
                    "error_message": ""
                })

            except Exception as exc:
                logger.error(f"Failed to fetch adj_factor for {trade_date}: {str(exc)}")
                self._report_progress({
                    "progress_percent": int(((day_idx + 1) / total_days) * 100),
                    "status": "running",
                    "current_task": f"{trade_date} 获取失败: {str(exc)}",
                    "trading_days_processed": day_idx + 1,
                    "trading_days_total": total_days,
                    "current_date": trade_date,
                    "error_message": str(exc)
                })

            time.sleep(0.2)

        self._report_progress({
            "progress_percent": 100,
            "status": "completed",
            "current_task": "复权因子历史数据清洗完成",
            "trading_days_processed": total_days,
            "trading_days_total": total_days,
            "current_date": "",
            "stock_processed": 0,
            "stock_total": 0,
            "error_message": ""
        })

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        date_str = (date_str or "").strip()
        if not date_str:
            raise ValueError("date is required")
        if len(date_str) == 10 and "-" in date_str:
            return date_str.replace("-", "")
        return date_str
