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
    
    def get_latest_report_period(self):
        """
        根据当前日期判断最新报告期
        
        财报披露时间规则：
        - 一季报(0331): 4月底前披露
        - 半年报(0630): 8月底前披露  
        - 三季报(0930): 10月底前披露
        - 年报(1231): 次年4月底前披露
        
        Returns:
            str: 最新报告期 YYYYMMDD格式
        """
        today = datetime.now()
        month = today.month
        year = today.year
        
        if month >= 5 and month < 9:
            return f"{year}0331"  # 一季报
        elif month >= 9 and month < 11:
            return f"{year}0630"  # 半年报
        elif month >= 11:
            return f"{year}0930"  # 三季报
        else:  # 1-4月
            return f"{year-1}1231"  # 上年年报
    
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
    
    def parse_periods_input(self, periods_input):
        """
        解析报告期输入
        
        Args:
            periods_input: 可以是：
                - 单个报告期: "20240930"
                - 报告期列表: ["20240331", "20240630"]
                - 报告期范围: {"start": "20240331", "end": "20240930"}
                - None: 使用最新报告期
                
        Returns:
            List[str]: 报告期列表
        """
        if periods_input is None:
            # 使用最新报告期
            return [self.get_latest_report_period()]
        elif isinstance(periods_input, str):
            # 单个报告期
            return [periods_input]
        elif isinstance(periods_input, list):
            # 报告期列表
            return periods_input
        elif isinstance(periods_input, dict):
            # 报告期范围
            return self.get_report_periods(periods_input['start'], periods_input['end'])
        else:
            raise ValueError(f"Invalid periods_input type: {type(periods_input)}")
    
    def validate_balance_sheet(self, row, log_warning=False):
        """
        验证资产负债表数据完整性：资产 = 负债 + 所有者权益
        
        Args:
            row: DataFrame row
            log_warning: 是否记录警告日志（默认False，由调用方汇总输出）
            
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
                    if log_warning:
                        logger.debug(
                            f"资产负债表验证失败: 资产={total_assets}, "
                            f"负债+权益={calculated_assets}, 差异={diff_percent*100:.4f}%"
                        )
                    return False
            return True
        except Exception as e:
            logger.debug(f"资产负债表验证异常: {str(e)}")
            return True  # 验证出错时仍然保存数据
    
    def clean_financial_income(self, symbols=None, periods=None, use_vip=None):
        """
        清洗利润表数据（智能选择普通/VIP接口）
        
        Args:
            symbols: 股票代码列表（tushare格式），None表示全市场
            periods: 报告期（支持多种格式，见parse_periods_input），None表示最新报告期
            use_vip: 是否使用VIP接口，None表示自动判断
        """
        # 解析报告期
        periods_list = self.parse_periods_input(periods)
        
        # 自动判断是否使用VIP接口
        if use_vip is None:
            if symbols is None:
                use_vip = True
            elif len(symbols) > 50:
                use_vip = True
                logger.info("💡 股票数量超过50只，自动使用VIP接口")
            else:
                use_vip = False
        
        if use_vip:
            return self._clean_income_with_vip(periods_list, symbols)
        else:
            return self._clean_income_with_normal(symbols, periods_list)
    
    def _clean_income_with_vip(self, periods, filter_symbols=None):
        """使用VIP接口清洗利润表数据"""
        logger.info(f"📊 [利润表-VIP] 开始清洗 {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'total_revenue', 'revenue', 'operate_profit', 'total_profit',
            'n_income', 'n_income_attr_p', 'basic_eps', 'diluted_eps',
        ]
        
        total_saved = 0
        missing_f_ann_date_count = 0
        
        with tqdm(total=len(periods), desc="📊 利润表(VIP)", ncols=100) as pbar:
            for period in periods:
                try:
                    # 使用VIP接口一次性获取所有股票
                    df = self.pro.income_vip(
                        period=period,
                        fields=','.join(core_fields)
                    )
                    
                    if df.empty:
                        pbar.set_postfix_str(f"{period}: 无数据")
                        pbar.update(1)
                        continue
                    
                    # 如果指定了股票列表，进行筛选
                    if filter_symbols:
                        df = df[df['ts_code'].isin(filter_symbols)]
                    
                    # PIT数据筛选：对每个(ts_code, end_date)保留最早的f_ann_date记录
                    if 'f_ann_date' in df.columns and not df['f_ann_date'].isna().all():
                        df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                    else:
                        missing_f_ann_date_count += 1
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
                            total_saved += len(upsert_operations)
                            pbar.set_postfix_str(f"{period}: 保存{len(upsert_operations)}条")
                    
                    pbar.update(1)
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"{period} 利润表处理失败: {str(e)}")
                    pbar.update(1)
                    continue
        
        if missing_f_ann_date_count > 0:
            logger.warning(f"⚠️  有 {missing_f_ann_date_count} 个报告期缺少f_ann_date，已使用update_flag筛选")
        
        logger.info(f"[利润表-VIP] 完成，共保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "periods": periods}
    
    def _clean_income_with_normal(self, symbols, periods):
        """使用普通接口清洗利润表数据"""
        logger.info(f"📊 [利润表] 开始清洗 {len(symbols)} 只股票 × {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'total_revenue', 'revenue', 'operate_profit', 'total_profit',
            'n_income', 'n_income_attr_p', 'basic_eps', 'diluted_eps',
        ]
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        total_saved = 0
        error_count = 0
        
        with tqdm(total=total_tasks, desc="📊 利润表", ncols=100) as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        df = self.pro.income(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # PIT数据筛选：保留最早的f_ann_date记录
                        if not df.empty:
                            df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            ensure_collection_and_indexes_financial('financial_income')
                            
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
                                total_saved += len(upsert_operations)
                        
                        processed += 1
                        pbar.update(1)
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"{symbol} {period}: {str(e)}")
                        error_count += 1
                        processed += 1
                        pbar.update(1)
                        continue
        
        if error_count > 0:
            logger.warning(f"⚠️  有 {error_count} 个任务失败")
        logger.info(f"[利润表] 完成 {processed}/{total_tasks} 个任务，保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "processed": processed, "total_tasks": total_tasks}
    
    def clean_financial_balance(self, symbols=None, periods=None, use_vip=None):
        """
        清洗资产负债表数据（智能选择普通/VIP接口）
        
        Args:
            symbols: 股票代码列表（tushare格式），None表示全市场
            periods: 报告期（支持多种格式），None表示最新报告期
            use_vip: 是否使用VIP接口，None表示自动判断
        """
        periods_list = self.parse_periods_input(periods)
        
        if use_vip is None:
            if symbols is None:
                use_vip = True
            elif len(symbols) > 50:
                use_vip = True
                logger.info("💡 股票数量超过50只，自动使用VIP接口")
            else:
                use_vip = False
        
        if use_vip:
            return self._clean_balance_with_vip(periods_list, symbols)
        else:
            return self._clean_balance_with_normal(symbols, periods_list)
    
    def _clean_balance_with_vip(self, periods, filter_symbols=None):
        """使用VIP接口清洗资产负债表数据"""
        logger.info(f"💰 [资产负债表-VIP] 开始清洗 {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'total_assets', 'total_cur_assets', 'total_nca',
            'total_liab', 'total_cur_liab', 'total_ncl',
            'total_hldr_eqy_exc_min_int', 'total_hldr_eqy_inc_min_int',
        ]
        
        total_saved = 0
        validation_failed = 0
        missing_f_ann_date_count = 0
        
        with tqdm(total=len(periods), desc="💰 资产负债表(VIP)", ncols=100) as pbar:
            for period in periods:
                try:
                    df = self.pro.balancesheet_vip(
                        period=period,
                        fields=','.join(core_fields)
                    )
                    
                    if df.empty:
                        pbar.set_postfix_str(f"{period}: 无数据")
                        pbar.update(1)
                        continue
                    
                    if filter_symbols:
                        df = df[df['ts_code'].isin(filter_symbols)]
                    
                    # PIT数据筛选：保留最早的f_ann_date记录
                    if 'f_ann_date' in df.columns and not df['f_ann_date'].isna().all():
                        df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                    else:
                        missing_f_ann_date_count += 1
                        df = df[df['update_flag'] == '0']
                    
                    if not df.empty:
                        df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                        df = df.rename(columns={'ts_code': 'symbol'})
                        
                        # 数据验证
                        for idx, row in df.iterrows():
                            if not self.validate_balance_sheet(row):
                                validation_failed += 1
                        
                        ensure_collection_and_indexes_financial('financial_balance')
                        
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
                            total_saved += len(upsert_operations)
                            pbar.set_postfix_str(f"{period}: 保存{len(upsert_operations)}条")
                    
                    pbar.update(1)
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"{period} 资产负债表处理失败: {str(e)}")
                    pbar.update(1)
                    continue
        
        if missing_f_ann_date_count > 0:
            logger.warning(f"⚠️  有 {missing_f_ann_date_count} 个报告期缺少f_ann_date，已使用update_flag筛选")
        if validation_failed > 0:
            logger.warning(f"⚠️  有 {validation_failed} 条记录验证失败（资产≠负债+权益）")
        
        logger.info(f"[资产负债表-VIP] 完成，共保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "validation_failed": validation_failed, "periods": periods}
    
    def _clean_balance_with_normal(self, symbols, periods):
        """使用普通接口清洗资产负债表数据"""
        logger.info(f"💰 [资产负债表] 开始清洗 {len(symbols)} 只股票 × {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'total_assets', 'total_cur_assets', 'total_nca',
            'total_liab', 'total_cur_liab', 'total_ncl',
            'total_hldr_eqy_exc_min_int', 'total_hldr_eqy_inc_min_int',
        ]
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        total_saved = 0
        validation_failed = 0
        error_count = 0
        
        with tqdm(total=total_tasks, desc="💰 资产负债表", ncols=100) as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        df = self.pro.balancesheet(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # PIT数据筛选：保留最早的f_ann_date记录
                        if not df.empty:
                            df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            for idx, row in df.iterrows():
                                if not self.validate_balance_sheet(row):
                                    validation_failed += 1
                            
                            ensure_collection_and_indexes_financial('financial_balance')
                            
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
                                total_saved += len(upsert_operations)
                        
                        processed += 1
                        pbar.update(1)
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"{symbol} {period}: {str(e)}")
                        error_count += 1
                        processed += 1
                        pbar.update(1)
                        continue
        
        if error_count > 0:
            logger.warning(f"⚠️  有 {error_count} 个任务失败")
        if validation_failed > 0:
            logger.warning(f"⚠️  有 {validation_failed} 条记录验证失败（资产≠负债+权益）")
        
        logger.info(f"[资产负债表] 完成 {processed}/{total_tasks} 个任务，保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "validation_failed": validation_failed, "processed": processed, "total_tasks": total_tasks}
    
    def clean_financial_cashflow(self, symbols=None, periods=None, use_vip=None):
        """
        清洗现金流量表数据（智能选择普通/VIP接口）
        
        Args:
            symbols: 股票代码列表（tushare格式），None表示全市场
            periods: 报告期（支持多种格式），None表示最新报告期
            use_vip: 是否使用VIP接口，None表示自动判断
        """
        periods_list = self.parse_periods_input(periods)
        
        if use_vip is None:
            if symbols is None:
                use_vip = True
            elif len(symbols) > 50:
                use_vip = True
                logger.info("💡 股票数量超过50只，自动使用VIP接口")
            else:
                use_vip = False
        
        if use_vip:
            return self._clean_cashflow_with_vip(periods_list, symbols)
        else:
            return self._clean_cashflow_with_normal(symbols, periods_list)
    
    def _clean_cashflow_with_vip(self, periods, filter_symbols=None):
        """使用VIP接口清洗现金流量表数据"""
        logger.info(f"[现金流量表-VIP] 开始清洗 {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'n_cashflow_act', 'n_cashflow_inv_act', 'n_cashflow_fnc_act',
            'c_cash_equ_end_period', 'c_cash_equ_beg_period',
        ]
        
        total_saved = 0
        missing_f_ann_date_count = 0
        
        with tqdm(total=len(periods), desc="现金流量表(VIP)", ncols=100) as pbar:
            for period in periods:
                try:
                    df = self.pro.cashflow_vip(
                        period=period,
                        fields=','.join(core_fields)
                    )
                    
                    if df.empty:
                        pbar.set_postfix_str(f"{period}: 无数据")
                        pbar.update(1)
                        continue
                    
                    if filter_symbols:
                        df = df[df['ts_code'].isin(filter_symbols)]
                    
                    # PIT数据筛选：保留最早的f_ann_date记录
                    if 'f_ann_date' in df.columns and not df['f_ann_date'].isna().all():
                        df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                    else:
                        missing_f_ann_date_count += 1
                        df = df[df['update_flag'] == '0']
                    
                    if not df.empty:
                        df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                        df = df.rename(columns={'ts_code': 'symbol'})
                        
                        ensure_collection_and_indexes_financial('financial_cashflow')
                        
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
                            total_saved += len(upsert_operations)
                            pbar.set_postfix_str(f"{period}: 保存{len(upsert_operations)}条")
                    
                    pbar.update(1)
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"{period} 现金流量表处理失败: {str(e)}")
                    pbar.update(1)
                    continue
        
        if missing_f_ann_date_count > 0:
            logger.warning(f"⚠️  有 {missing_f_ann_date_count} 个报告期缺少f_ann_date，已使用update_flag筛选")
        
        logger.info(f"[现金流量表-VIP] 完成，共保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "periods": periods}
    
    def _clean_cashflow_with_normal(self, symbols, periods):
        """使用普通接口清洗现金流量表数据"""
        logger.info(f"[现金流量表] 开始清洗 {len(symbols)} 只股票 × {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'f_ann_date', 'report_type', 'update_flag',
            'n_cashflow_act', 'n_cashflow_inv_act', 'n_cashflow_fnc_act',
            'c_cash_equ_end_period', 'c_cash_equ_beg_period',
        ]
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        total_saved = 0
        error_count = 0
        
        with tqdm(total=total_tasks, desc="现金流量表", ncols=100) as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        df = self.pro.cashflow(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        
                        # PIT数据筛选：保留最早的f_ann_date记录
                        if not df.empty:
                            df = df.sort_values('f_ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            ensure_collection_and_indexes_financial('financial_cashflow')
                            
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
                                total_saved += len(upsert_operations)
                        
                        processed += 1
                        pbar.update(1)
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"{symbol} {period}: {str(e)}")
                        error_count += 1
                        processed += 1
                        pbar.update(1)
                        continue
        
        if error_count > 0:
            logger.warning(f"有 {error_count} 个任务失败")
        logger.info(f"[现金流量表] 完成 {processed}/{total_tasks} 个任务，保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "processed": processed, "total_tasks": total_tasks}
    
    def clean_financial_indicator(self, symbols=None, periods=None, use_vip=None):
        """
        清洗财务指标数据（智能选择普通/VIP接口）
        
        Args:
            symbols: 股票代码列表（tushare格式），None表示全市场
            periods: 报告期（支持多种格式），None表示最新报告期
            use_vip: 是否使用VIP接口，None表示自动判断
        """
        periods_list = self.parse_periods_input(periods)
        
        if use_vip is None:
            if symbols is None:
                use_vip = True
            elif len(symbols) > 50:
                use_vip = True
                logger.info("股票数量超过50只，自动使用VIP接口")
            else:
                use_vip = False
        
        if use_vip:
            return self._clean_indicator_with_vip(periods_list, symbols)
        else:
            return self._clean_indicator_with_normal(symbols, periods_list)
    
    def _clean_indicator_with_vip(self, periods, filter_symbols=None):
        """使用VIP接口清洗财务指标数据"""
        logger.info(f"[财务指标-VIP] 开始清洗 {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'update_flag',
            'roe', 'roe_waa', 'roe_dt', 'roa',
            'gross_margin', 'netprofit_margin', 'debt_to_assets',
            'current_ratio', 'quick_ratio', 'q_roe', 'q_dt_roe',
        ]
        
        total_saved = 0
        missing_f_ann_date_count = 0
        
        with tqdm(total=len(periods), desc="财务指标(VIP)", ncols=100) as pbar:
            for period in periods:
                try:
                    df = self.pro.fina_indicator_vip(
                        period=period,
                        fields=','.join(core_fields)
                    )
                    
                    if df.empty:
                        pbar.set_postfix_str(f"{period}: 无数据")
                        pbar.update(1)
                        continue
                    
                    if filter_symbols:
                        df = df[df['ts_code'].isin(filter_symbols)]
                    
                    # PIT数据筛选：保留最早的f_ann_date记录
                    if not df.empty:
                        df = df.sort_values('ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                        df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                        df = df.rename(columns={'ts_code': 'symbol'})
                        
                        ensure_collection_and_indexes_financial('financial_indicator')
                        
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
                            total_saved += len(upsert_operations)
                            pbar.set_postfix_str(f"{period}: 保存{len(upsert_operations)}条")
                    
                    pbar.update(1)
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"{period} 财务指标处理失败: {str(e)}")
                    pbar.update(1)
                    continue
        
        if missing_f_ann_date_count > 0:
            logger.warning(f"⚠️  有 {missing_f_ann_date_count} 个报告期缺少f_ann_date，已使用update_flag筛选")
        
        logger.info(f"[财务指标-VIP] 完成，共保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "periods": periods}
    
    def _clean_indicator_with_normal(self, symbols, periods):
        """使用普通接口清洗财务指标数据"""
        logger.info(f"📈 [财务指标] 开始清洗 {len(symbols)} 只股票 × {len(periods)} 个报告期")
        
        core_fields = [
            'ts_code', 'end_date', 'ann_date', 'update_flag',
            'roe', 'roe_waa', 'roe_dt', 'roa',
            'gross_margin', 'netprofit_margin', 'debt_to_assets',
            'current_ratio', 'quick_ratio', 'q_roe', 'q_dt_roe',
        ]
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        total_saved = 0
        error_count = 0
        
        with tqdm(total=total_tasks, desc="📈 财务指标", ncols=100) as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        df = self.pro.fina_indicator(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(core_fields)
                        )
                        if not df.empty:
                            df = df.sort_values('ann_date').groupby(['ts_code', 'end_date']).first().reset_index()
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            ensure_collection_and_indexes_financial('financial_indicator')
                            
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
                                total_saved += len(upsert_operations)
                        
                        processed += 1
                        pbar.update(1)
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"{symbol} {period}: {str(e)}")
                        error_count += 1
                        processed += 1
                        pbar.update(1)
                        continue
        
        if error_count > 0:
            logger.warning(f"⚠️  有 {error_count} 个任务失败")
        logger.info(f"[财务指标] 完成 {processed}/{total_tasks} 个任务，保存 {total_saved} 条记录")
        return {"total_saved": total_saved, "processed": processed, "total_tasks": total_tasks}

