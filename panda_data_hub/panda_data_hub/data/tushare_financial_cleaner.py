from abc import ABC
import tushare as ts
from pymongo import UpdateOne
import traceback
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import time

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_common.utils.stock_utils import get_exchange_suffix
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes_financial


class TSFinancialCleaner(ABC):
    """Tushare财务数据清洗器"""
    
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        try:
            ts.set_token(config['TS_TOKEN'])
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise
    
    def get_report_periods(self, start_date, end_date):
        """
        获取指定日期范围内的所有报告期
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            
        Returns:
            List of report periods in YYYYMMDD format
        """
        start = datetime.strptime(str(start_date), "%Y%m%d")
        end = datetime.strptime(str(end_date), "%Y%m%d")
        
        periods = []
        current_year = start.year
        
        while current_year <= end.year:
            # 一季报
            if datetime(current_year, 3, 31) >= start and datetime(current_year, 3, 31) <= end:
                periods.append(f"{current_year}0331")
            # 半年报
            if datetime(current_year, 6, 30) >= start and datetime(current_year, 6, 30) <= end:
                periods.append(f"{current_year}0630")
            # 三季报
            if datetime(current_year, 9, 30) >= start and datetime(current_year, 9, 30) <= end:
                periods.append(f"{current_year}0930")
            # 年报
            if datetime(current_year, 12, 31) >= start and datetime(current_year, 12, 31) <= end:
                periods.append(f"{current_year}1231")
            
            current_year += 1
        
        return periods
    
    def validate_balance_sheet(self, row):
        """
        验证资产负债表数据完整性：资产 = 负债 + 所有者权益
        
        Args:
            row: DataFrame row
            
        Returns:
            bool: 验证是否通过
        """
        try:
            total_assets = row.get('total_assets', 0) or 0
            total_liab = row.get('total_liab', 0) or 0
            total_hldr_eqy_exc_min_int = row.get('total_hldr_eqy_exc_min_int', 0) or 0
            
            # 如果都是0或None，跳过验证
            if total_assets == 0 and total_liab == 0 and total_hldr_eqy_exc_min_int == 0:
                return True
            
            # 计算差异百分比（允许0.1%的误差）
            calculated_assets = total_liab + total_hldr_eqy_exc_min_int
            if total_assets != 0:
                diff_percent = abs((total_assets - calculated_assets) / total_assets)
                if diff_percent > 0.001:  # 0.1%的误差容忍
                    logger.warning(
                        f"Balance sheet validation failed: "
                        f"Assets={total_assets}, Liab+Equity={calculated_assets}, "
                        f"Diff={diff_percent*100:.4f}%"
                    )
                    return False
            return True
        except Exception as e:
            logger.error(f"Balance sheet validation error: {str(e)}")
            return True  # 验证出错时仍然保存数据
    
    def clean_financial_income(self, symbols, start_date, end_date):
        """
        清洗利润表数据
        
        Args:
            symbols: 股票代码列表（tushare格式，如：['000001.SZ', '600000.SH']）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        logger.info(f"开始清洗利润表数据，股票数量: {len(symbols)}")
        
        # 获取所有报告期
        periods = self.get_report_periods(start_date, end_date)
        logger.info(f"需要处理的报告期: {periods}")
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        
        # 核心字段
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'report_type', 'update_flag',
            'total_revenue',        # 营业总收入
            'revenue',              # 营业收入
            'operate_profit',       # 营业利润
            'total_profit',         # 利润总额
            'n_income',             # 净利润
            'n_income_attr_p',      # 归属于母公司所有者的净利润
            'basic_eps',            # 基本每股收益
            'diluted_eps',          # 稀释每股收益
        ]
        
        with tqdm(total=total_tasks, desc="清洗利润表数据") as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        # 调用tushare接口，只获取初次公告数据
                        df = self.pro.income(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # 只保留初次公告数据
                        df = df[df['update_flag'] == '0']
                        
                        if not df.empty:
                            # 转换股票代码格式
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            # 确保集合和索引存在
                            ensure_collection_and_indexes_financial('financial_income')
                            
                            # 批量插入/更新
                            upsert_operations = []
                            for _, row in df.iterrows():
                                record = row.to_dict()
                                upsert_operations.append(UpdateOne(
                                    {
                                        'symbol': record['symbol'],
                                        'end_date': record['end_date'],
                                        'ann_date': record['ann_date']
                                    },
                                    {'$set': record},
                                    upsert=True
                                ))
                            
                            if upsert_operations:
                                self.db_handler.mongo_client[self.config["MONGO_DB"]]['financial_income'].bulk_write(
                                    upsert_operations
                                )
                                logger.info(f"成功保存 {symbol} {period} 利润表数据，共 {len(upsert_operations)} 条")
                        
                        processed += 1
                        pbar.update(1)
                        
                        # 避免调用频率过快
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} {period} 利润表数据失败: {str(e)}")
                        processed += 1
                        pbar.update(1)
                        continue
        
        logger.info(f"利润表数据清洗完成，处理 {processed}/{total_tasks} 个任务")
    
    def clean_financial_balance(self, symbols, start_date, end_date):
        """
        清洗资产负债表数据
        
        Args:
            symbols: 股票代码列表（tushare格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        logger.info(f"开始清洗资产负债表数据，股票数量: {len(symbols)}")
        
        periods = self.get_report_periods(start_date, end_date)
        logger.info(f"需要处理的报告期: {periods}")
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        validation_failed = 0
        
        # 核心字段
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'report_type', 'update_flag',
            'total_assets',                  # 资产总计
            'total_cur_assets',              # 流动资产合计
            'total_nca',                     # 非流动资产合计
            'total_liab',                    # 负债合计
            'total_cur_liab',                # 流动负债合计
            'total_ncl',                     # 非流动负债合计
            'total_hldr_eqy_exc_min_int',    # 股东权益合计(不含少数股东权益)
            'total_hldr_eqy_inc_min_int',    # 股东权益合计(含少数股东权益)
        ]
        
        with tqdm(total=total_tasks, desc="清洗资产负债表数据") as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        # 调用tushare接口
                        df = self.pro.balancesheet(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # 只保留初次公告数据
                        df = df[df['update_flag'] == '0']
                        
                        if not df.empty:
                            # 转换股票代码格式
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            # 数据验证
                            for idx, row in df.iterrows():
                                if not self.validate_balance_sheet(row):
                                    validation_failed += 1
                            
                            # 确保集合和索引存在
                            ensure_collection_and_indexes_financial('financial_balance')
                            
                            # 批量插入/更新
                            upsert_operations = []
                            for _, row in df.iterrows():
                                record = row.to_dict()
                                upsert_operations.append(UpdateOne(
                                    {
                                        'symbol': record['symbol'],
                                        'end_date': record['end_date'],
                                        'ann_date': record['ann_date']
                                    },
                                    {'$set': record},
                                    upsert=True
                                ))
                            
                            if upsert_operations:
                                self.db_handler.mongo_client[self.config["MONGO_DB"]]['financial_balance'].bulk_write(
                                    upsert_operations
                                )
                                logger.info(f"成功保存 {symbol} {period} 资产负债表数据，共 {len(upsert_operations)} 条")
                        
                        processed += 1
                        pbar.update(1)
                        
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} {period} 资产负债表数据失败: {str(e)}")
                        processed += 1
                        pbar.update(1)
                        continue
        
        logger.info(f"资产负债表数据清洗完成，处理 {processed}/{total_tasks} 个任务，验证失败 {validation_failed} 条")
    
    def clean_financial_cashflow(self, symbols, start_date, end_date):
        """
        清洗现金流量表数据
        
        Args:
            symbols: 股票代码列表（tushare格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        logger.info(f"开始清洗现金流量表数据，股票数量: {len(symbols)}")
        
        periods = self.get_report_periods(start_date, end_date)
        logger.info(f"需要处理的报告期: {periods}")
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        
        # 核心字段
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'report_type', 'update_flag',
            'n_cashflow_act',        # 经营活动产生的现金流量净额
            'n_cashflow_inv_act',    # 投资活动产生的现金流量净额
            'n_cashflow_fnc_act',    # 筹资活动产生的现金流量净额
            'c_cash_equ_end_period', # 期末现金及现金等价物余额
            'c_cash_equ_beg_period', # 期初现金及现金等价物余额
        ]
        
        with tqdm(total=total_tasks, desc="清洗现金流量表数据") as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        # 调用tushare接口
                        df = self.pro.cashflow(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # 只保留初次公告数据
                        df = df[df['update_flag'] == '0']
                        
                        if not df.empty:
                            # 转换股票代码格式
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            # 确保集合和索引存在
                            ensure_collection_and_indexes_financial('financial_cashflow')
                            
                            # 批量插入/更新
                            upsert_operations = []
                            for _, row in df.iterrows():
                                record = row.to_dict()
                                upsert_operations.append(UpdateOne(
                                    {
                                        'symbol': record['symbol'],
                                        'end_date': record['end_date'],
                                        'ann_date': record['ann_date']
                                    },
                                    {'$set': record},
                                    upsert=True
                                ))
                            
                            if upsert_operations:
                                self.db_handler.mongo_client[self.config["MONGO_DB"]]['financial_cashflow'].bulk_write(
                                    upsert_operations
                                )
                                logger.info(f"成功保存 {symbol} {period} 现金流量表数据，共 {len(upsert_operations)} 条")
                        
                        processed += 1
                        pbar.update(1)
                        
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} {period} 现金流量表数据失败: {str(e)}")
                        processed += 1
                        pbar.update(1)
                        continue
        
        logger.info(f"现金流量表数据清洗完成，处理 {processed}/{total_tasks} 个任务")
    
    def clean_financial_indicator(self, symbols, start_date, end_date):
        """
        清洗财务指标数据
        
        Args:
            symbols: 股票代码列表（tushare格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        logger.info(f"开始清洗财务指标数据，股票数量: {len(symbols)}")
        
        periods = self.get_report_periods(start_date, end_date)
        logger.info(f"需要处理的报告期: {periods}")
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        
        # 核心字段
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'update_flag',
            'roe',                   # 净资产收益率
            'roe_waa',               # 加权平均净资产收益率
            'roe_dt',                # 净资产收益率(扣除非经常损益)
            'roa',                   # 总资产报酬率
            'gross_margin',          # 销售毛利率
            'netprofit_margin',      # 销售净利率
            'debt_to_assets',        # 资产负债率
            'current_ratio',         # 流动比率
            'quick_ratio',           # 速动比率
        ]
        
        with tqdm(total=total_tasks, desc="清洗财务指标数据") as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        # 调用tushare接口
                        df = self.pro.fina_indicator(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # 只保留初次公告数据
                        df = df[df['update_flag'] == '0']
                        
                        if not df.empty:
                            # 转换股票代码格式
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            # 确保集合和索引存在
                            ensure_collection_and_indexes_financial('financial_indicator')
                            
                            # 批量插入/更新
                            upsert_operations = []
                            for _, row in df.iterrows():
                                record = row.to_dict()
                                upsert_operations.append(UpdateOne(
                                    {
                                        'symbol': record['symbol'],
                                        'end_date': record['end_date'],
                                        'ann_date': record['ann_date']
                                    },
                                    {'$set': record},
                                    upsert=True
                                ))
                            
                            if upsert_operations:
                                self.db_handler.mongo_client[self.config["MONGO_DB"]]['financial_indicator'].bulk_write(
                                    upsert_operations
                                )
                                logger.info(f"成功保存 {symbol} {period} 财务指标数据，共 {len(upsert_operations)} 条")
                        
                        processed += 1
                        pbar.update(1)
                        
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} {period} 财务指标数据失败: {str(e)}")
                        processed += 1
                        pbar.update(1)
                        continue
        
        logger.info(f"财务指标数据清洗完成，处理 {processed}/{total_tasks} 个任务")

