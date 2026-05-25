import time
from typing import List, Optional, Union

import pandas as pd

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger


class AdjFactorDataReader:
    """复权因子数据读取器，从 MongoDB adj_factor 集合读取数据"""

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.all_symbols = self._get_all_symbols()

    def _get_all_symbols(self):
        try:
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"], "adj_factor"
            )
            return collection.distinct("symbol")
        except Exception:
            return []

    def get_adj_factor(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取复权因子数据

        Args:
            symbols: 股票代码或代码列表，为 None 时返回所有股票
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD

        Returns:
            DataFrame with columns: date, symbol, adj_factor
        """
        start_time = time.time()

        if isinstance(symbols, str):
            symbols = [symbols]

        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"], "adj_factor"
        )

        query = {}
        if symbols:
            query["symbol"] = {"$in": symbols}
        if start_date and end_date:
            query["date"] = {"$gte": start_date, "$lte": end_date}
        elif start_date:
            query["date"] = {"$gte": start_date}
        elif end_date:
            query["date"] = {"$lte": end_date}

        projection = {"_id": 0, "date": 1, "symbol": 1, "adj_factor": 1}

        cursor = collection.find(query, projection=projection)
        df = pd.DataFrame(list(cursor))

        if df.empty:
            logger.warning("No adj_factor data found for the specified parameters")
            return None

        end_time = time.time()
        logger.info(f"Adj factor data query took {end_time - start_time:.2f} seconds, {len(df)} rows")
        return df

    def get_all_symbols(self):
        return self.all_symbols
