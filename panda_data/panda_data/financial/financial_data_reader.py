import pandas as pd
import time
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
import concurrent.futures
from datetime import datetime
from typing import List, Optional, Dict, Any


class FinancialDataReader:
    """财务数据读取器"""
    
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        
        # 数据类型到集合名称的映射
        self.collection_mapping = {
            'income': 'financial_income',
            'balance': 'financial_balance',
            'cashflow': 'financial_cashflow',
            'indicator': 'financial_indicator'
        }
    
    def get_financial_data(
        self,
        symbols=None,
        start_date=None,
        end_date=None,
        fields=None,
        data_type='indicator',
        date_type='ann_date'
    ):
        """
        获取财务数据
        
        Args:
            symbols: 股票代码列表或单个股票代码（如：['000001.SZ'] 或 '000001.SZ'）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            fields: 需要的字段列表（如：['roe', 'roa']），None表示所有字段
            data_type: 数据类型，可选值：'income', 'balance', 'cashflow', 'indicator'
            date_type: 日期类型，可选值：'ann_date'（公告日期）, 'end_date'（报告期）
        
        Returns:
            pandas DataFrame，包含财务数据
        """
        start_time = time.time()
        
        # 参数验证
        if data_type not in self.collection_mapping:
            logger.error(f"不支持的数据类型: {data_type}")
            return None
        
        if date_type not in ['ann_date', 'end_date']:
            logger.error(f"不支持的日期类型: {date_type}")
            return None
        
        # 转换参数
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(fields, str):
            fields = [fields]
        
        # 获取集合
        collection_name = self.collection_mapping[data_type]
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            collection_name
        )
        
        # 构建查询条件
        query = {}
        
        # 添加股票代码过滤
        if symbols:
            query["symbol"] = {"$in": symbols}
        
        # 添加日期过滤
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query["$gte"] = str(start_date)
            if end_date:
                date_query["$lte"] = str(end_date)
            query[date_type] = date_query
        
        # 构建投影（指定返回字段）
        projection = None
        if fields:
            # 基础字段必须包含
            base_fields = ['symbol', 'end_date', 'ann_date']
            all_fields = list(set(base_fields + fields))
            projection = {field: 1 for field in all_fields}
            projection['_id'] = 0
        else:
            projection = {'_id': 0}
        
        # 查询数据
        try:
            cursor = collection.find(query, projection)
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                logger.warning(
                    f"未找到财务数据: type={data_type}, symbols={symbols}, "
                    f"date_range=[{start_date}, {end_date}], date_type={date_type}"
                )
                return None
            
            # 按日期排序
            df = df.sort_values([date_type, 'symbol'])
            
            end_time = time.time()
            logger.info(
                f"财务数据查询完成: type={data_type}, records={len(df)}, "
                f"time={end_time - start_time:.2f}s"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"查询财务数据失败: {str(e)}")
            return None
    
    def get_latest_financial_data(
        self,
        symbols=None,
        fields=None,
        data_type='indicator',
        as_of_date=None
    ):
        """
        获取最新的财务数据（截止到指定日期的最新公告）
        
        Args:
            symbols: 股票代码列表
            fields: 需要的字段列表
            data_type: 数据类型
            as_of_date: 截止日期（公告日期），None表示当前日期
        
        Returns:
            pandas DataFrame，每只股票只返回一条最新的财务数据
        """
        if as_of_date is None:
            as_of_date = datetime.now().strftime("%Y%m%d")
        
        # 获取所有符合条件的数据
        df = self.get_financial_data(
            symbols=symbols,
            end_date=as_of_date,
            fields=fields,
            data_type=data_type,
            date_type='ann_date'
        )
        
        if df is None or df.empty:
            return None
        
        # 按股票代码分组，取每个股票最新的公告日期
        df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')
        latest_df = df.sort_values('ann_date').groupby('symbol').tail(1)
        
        logger.info(f"获取最新财务数据: type={data_type}, stocks={len(latest_df)}")
        
        return latest_df
    
    def get_financial_data_by_quarter(
        self,
        symbols=None,
        quarters=None,
        fields=None,
        data_type='indicator'
    ):
        """
        按报告期获取财务数据
        
        Args:
            symbols: 股票代码列表
            quarters: 报告期列表（如：['20231231', '20230930']）
            fields: 需要的字段列表
            data_type: 数据类型
        
        Returns:
            pandas DataFrame
        """
        if quarters is None:
            logger.error("必须指定报告期")
            return None
        
        if isinstance(quarters, str):
            quarters = [quarters]
        
        # 获取集合
        collection_name = self.collection_mapping[data_type]
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            collection_name
        )
        
        # 构建查询条件
        query = {"end_date": {"$in": quarters}}
        
        if symbols:
            query["symbol"] = {"$in": symbols}
        
        # 构建投影
        projection = None
        if fields:
            base_fields = ['symbol', 'end_date', 'ann_date', 'report_type', 'update_flag']
            all_fields = list(set(base_fields + fields))
            projection = {field: 1 for field in all_fields}
            projection['_id'] = 0
        else:
            projection = {'_id': 0}
        
        # 查询数据
        try:
            cursor = collection.find(query, projection)
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                logger.warning(f"未找到财务数据: type={data_type}, quarters={quarters}")
                return None
            
            df = df.sort_values(['end_date', 'symbol'])
            
            logger.info(f"按季度查询财务数据: type={data_type}, records={len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"按季度查询财务数据失败: {str(e)}")
            return None
    
    def get_financial_time_series(
        self,
        symbol,
        fields,
        data_type='indicator',
        start_date=None,
        end_date=None,
        date_type='ann_date'
    ):
        """
        获取单只股票的财务数据时间序列
        
        Args:
            symbol: 股票代码
            fields: 需要的字段列表
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            date_type: 日期类型
        
        Returns:
            pandas DataFrame，索引为日期，列为字段
        """
        df = self.get_financial_data(
            symbols=[symbol],
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_type=data_type,
            date_type=date_type
        )
        
        if df is None or df.empty:
            return None
        
        # 设置日期为索引
        df[date_type] = pd.to_datetime(df[date_type], format='%Y%m%d')
        df = df.set_index(date_type)
        
        # 只保留数值字段
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        df = df[numeric_cols]
        
        return df
    
    def get_financial_cross_section(
        self,
        date,
        fields,
        data_type='indicator',
        date_type='ann_date',
        symbols=None
    ):
        """
        获取某个时间点的横截面财务数据（所有股票在某一时点的财务数据）
        
        Args:
            date: 日期 YYYYMMDD
            fields: 需要的字段列表
            data_type: 数据类型
            date_type: 日期类型
            symbols: 股票代码列表，None表示所有股票
        
        Returns:
            pandas DataFrame，每行代表一只股票
        """
        # 如果是公告日期，获取截止到该日期的最新数据
        if date_type == 'ann_date':
            return self.get_latest_financial_data(
                symbols=symbols,
                fields=fields,
                data_type=data_type,
                as_of_date=date
            )
        else:
            # 如果是报告期，直接查询该报告期的数据
            return self.get_financial_data_by_quarter(
                symbols=symbols,
                quarters=[date],
                fields=fields,
                data_type=data_type
            )
    
    def get_all_symbols(self):
        """获取所有有财务数据的股票代码"""
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            "financial_indicator"  # 使用财务指标表
        )
        return collection.distinct("symbol")

