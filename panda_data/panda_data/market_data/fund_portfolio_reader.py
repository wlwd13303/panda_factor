"""
基金列表
panda.fund_basic

{
  "_id": {
    "$oid": "6826f6c84198cee20598e66b"
  },
  "ts_code": "021994.OF",
  "name": "财通资管康泽稳健养老目标一年持有Y",
  "management": "财通证券资管",
  "custodian": "中国工商银行",
  "fund_type": "混合型",
  "found_date": 20250430,
  "m_fee": 0.3,
  "c_fee": 0.075,
  "p_value": 1,
  "benchmark": "中债综合(全价)指数收益率*80%+沪深300指数收益率*20%",
  "status": "L",
  "invest_type": "混合型",
  "type": "契约型开放式",
  "purc_startdate": 20250430,
  "market": "O"
}

基金持仓数据
panda.fund_portfolio
{
  "_id": {
    "$oid": "68270bbf140bee093c861c3e"
  },
  "ts_code": "159213.SZ",
  "ann_date": 20250425,
  "end_date": 20250423,
  "symbol": "002139.SZ",
  "mkv": 2163990,
  "amount": 159000,
  "stk_mkv_ratio": 2.82,
  "stk_float_ratio": 0.01
}

"""
from datetime import datetime

import pandas as pd

from panda_common.client import MongoClient
from typing import List, Dict, Optional


class FundPortfolioReader(object):
    def __init__(self, config: Dict):
        self.__config = config
        self.__mongo = MongoClient(self.__config)  # 修正配置参数引用
        self.__collection_fund_pro = self.__mongo.getCollection("fund_portfolio")
        self.__collection_fund_basic = self.__mongo.getCollection("fund_basic")
    # 获取公募基金持仓数据

    def get_fund_pro(self, ts_code: str, start_date:str=None, end_date=None) -> pd.DataFrame:
        """
        获取指定基金的持仓数据（返回DataFrame）
        :param ts_code: 基金代码 (e.g. "159213.SZ")
        :return: 持仓数据DataFrame，包含以下字段：
                 ts_code, ann_date, end_date, symbol,
                 mkv, amount, stk_mkv_ratio, stk_float_ratio
        """
        if ts_code is not None:
            query = {"ts_code": ts_code, }
        # start = datetime.strptime(start_date, "%Y%m%d")
        # end = datetime.strptime(end_date, "%Y%m%d")
        if start_date or end_date:
            ann_date_query = {}
            if start_date:
                # 验证日期格式并转换为整数
                if not start_date.isdigit() or len(start_date) != 8:
                    raise ValueError("start_date 必须为 YYYYMMDD 格式的字符串（如 '20240101'）")
                ann_date_query["$gte"] = int(start_date)
            if end_date:
                # 验证日期格式并转换为整数
                if not end_date.isdigit() or len(end_date) != 8:
                    raise ValueError("end_date 必须为 YYYYMMDD 格式的字符串（如 '20241231'）")
                ann_date_query["$lte"] = int(end_date)
            query["end_date"] = ann_date_query

        # 排除_id字段并指定返回字段
        projection = {
            "_id": 0,
            "ts_code": 1,
            "ann_date": 1,
            "end_date": 1,
            "symbol": 1,
            "mkv": 1,
            "amount": 1,
            "stk_mkv_ratio": 1,
            "stk_float_ratio": 1
        }

        cursor = self.__collection_fund_pro.find(query, projection)
        df = pd.DataFrame(list(cursor))

        # 日期字段格式转换（可选）
        if not df.empty:
            date_cols = ["ann_date", "end_date"]
            df[date_cols] = df[date_cols].astype(str)

        return df

    def get_all_funds(self, ts_code: Optional[str] = None) -> pd.DataFrame:
        """
        获取基金基本信息列表（返回DataFrame）
        :param ts_code: 可选基金代码（精确查询）
        :return: 基金信息DataFrame，包含以下字段：
                 ts_code, name, management, custodian,
                 fund_type, found_date, m_fee, c_fee,
                 p_value, benchmark, status, invest_type,
                 type, purc_startdate, market
        """
        query = {"ts_code": ts_code} if ts_code else {}
        projection = {
            "_id": 0,
            "ts_code": 1,
            "name": 1,
            "management": 1,
            "custodian": 1,
            "fund_type": 1,
            "found_date": 1,
            "m_fee": 1,
            "c_fee": 1,
            "p_value": 1,
            "benchmark": 1,
            "status": 1,
            "invest_type": 1,
            "type": 1,
            "purc_startdate": 1,
            "market": 1
        }

        cursor = self.__collection_fund_basic.find(query, projection)
        df = pd.DataFrame(list(cursor))

        # 处理日期字段（可选）
        if not df.empty:
            date_cols = ["found_date", "purc_startdate"]
            df[date_cols] = df[date_cols].astype(str)

        return df

    def get_all_funds(self, ts_code: str = None) -> pd.DataFrame:
        """
        获取基金基本信息列表（返回DataFrame）
        :param ts_code: 可选基金代码（精确查询）
        :return: 基金信息DataFrame，包含以下字段：
                ts_code, name, management, custodian, fund_type,
                found_date, m_fee, c_fee, p_value, benchmark,
                status, invest_type, type, purc_startdate, market
        """
        # 构建查询条件
        query = {"ts_code": ts_code} if ts_code else {}

        # 显式指定返回字段（白名单机制）
        projection = {
            "_id": 0,
            "ts_code": 1,
            "name": 1,
            "management": 1,
            "custodian": 1,
            "fund_type": 1,
            "found_date": 1,
            "m_fee": 1,
            "c_fee": 1,
            "p_value": 1,
            "benchmark": 1,
            "status": 1,
            "invest_type": 1,
            "type": 1,
            "purc_startdate": 1,
            "market": 1
        }

        # 执行基础查询
        cursor = self.__collection_fund_basic.find(query, projection)
        df = pd.DataFrame(list(cursor))

        # 处理日期字段格式转换（数值转字符串）
        if not df.empty:
            date_cols = ["found_date", "purc_startdate"]
            df[date_cols] = df[date_cols].astype(str)

        return df

