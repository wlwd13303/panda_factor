from abc import ABC
import tushare as ts
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.data.tushare_financial_cleaner import TSFinancialCleaner


class FinancialCleanTSService(ABC):
    """Tushare财务数据清洗服务"""
    
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.cleaner = TSFinancialCleaner(config)
        self.progress_callback = None
        
        try:
            ts.set_token(config['TS_TOKEN'])
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise
    
    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback
    
    def get_stocks_list(self, symbols=None):
        """
        获取股票列表
        
        Args:
            symbols: 指定的股票代码列表（pandas格式，如：['000001.SZ', '600000.SH']）
                     如果为None，则从数据库获取所有股票
        
        Returns:
            list: tushare格式的股票代码列表
        """
        if symbols:
            # 转换为tushare格式
            ts_symbols = []
            for symbol in symbols:
                if '.SZ' in symbol or '.SH' in symbol:
                    ts_symbols.append(symbol)
                else:
                    # 如果没有后缀，尝试添加
                    if symbol.startswith('6'):
                        ts_symbols.append(f"{symbol}.SH")
                    else:
                        ts_symbols.append(f"{symbol}.SZ")
            return ts_symbols
        else:
            # 从数据库获取所有股票
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stocks"
            )
            stocks = collection.distinct("symbol")
            
            # 转换为tushare格式
            ts_symbols = []
            for symbol in stocks:
                if '.SZ' in symbol or '.SH' in symbol:
                    ts_symbols.append(symbol)
            
            return ts_symbols
    
    def get_recent_quarters(self, num_quarters=2):
        """
        获取最近N个季度的报告期
        
        Args:
            num_quarters: 季度数量，默认2个季度
            
        Returns:
            list: 报告期列表，格式：['20231231', '20230930']
        """
        today = datetime.now()
        quarters = []
        
        # 定义季度月份
        quarter_months = [3, 6, 9, 12]
        quarter_days = [31, 30, 30, 31]
        
        # 找到当前或最近的已结束季度
        current_year = today.year
        current_month = today.month
        
        # 从最近的季度开始往回推
        for year in range(current_year, current_year - 3, -1):  # 最多往回推3年
            for i in range(len(quarter_months) - 1, -1, -1):
                q_month = quarter_months[i]
                q_day = quarter_days[i]
                
                # 只添加已经结束的季度
                quarter_end = datetime(year, q_month, q_day)
                if quarter_end < today:
                    quarters.append(f"{year}{q_month:02d}{q_day:02d}")
                    
                    if len(quarters) >= num_quarters:
                        return quarters
        
        return quarters
    
    def financial_history_clean(self, start_date, end_date, symbols=None, data_types=None):
        """
        历史财务数据清洗（旧接口，保持兼容性）
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            symbols: 股票代码列表（pandas格式），如：['000001.SZ', '600000.SH']
            data_types: 数据类型列表，如：['income', 'balance', 'cashflow', 'indicator']
                       如果为None，则清洗所有类型
        """
        logger.info(f"📅 历史数据清洗: {start_date} ~ {end_date}")
        # 转换为报告期范围格式
        periods = {"start": start_date, "end": end_date}
        return self.clean_financial_by_periods(symbols, periods, data_types)
    
    def clean_financial_by_periods(self, symbols=None, periods=None, data_types=None, use_vip=None):
        """
        按报告期清洗财务数据（支持VIP接口）
        
        Args:
            symbols: 股票代码列表（pandas格式），如：['000001.SZ', '600000.SH']
                     None表示全市场
            periods: 报告期，支持以下格式：
                     - None: 使用最新报告期
                     - str: 单个报告期，如："20240930"
                     - list: 报告期列表，如：["20240331", "20240630"]
                     - dict: 报告期范围，如：{"start": "20240331", "end": "20240930"}
            data_types: 数据类型列表，如：['income', 'balance', 'cashflow', 'indicator']
                       如果为None，则清洗所有类型
            use_vip: 是否强制使用VIP接口，None表示自动判断
        """
        # 获取股票列表（如果指定了股票）
        ts_symbols = self.get_stocks_list(symbols) if symbols else None
        
        # 判断是否使用VIP接口
        if ts_symbols is None:
            stock_info = "全市场"
            stock_count = "全部"
        else:
            stock_info = f"{len(ts_symbols)}只股票"
            stock_count = len(ts_symbols)
        
        # 默认清洗所有类型
        if data_types is None:
            data_types = ['income', 'balance', 'cashflow', 'indicator']
        
        total_types = len(data_types)
        
        # 解析报告期信息用于日志
        periods_info = self._format_periods_info(periods)
        
        logger.info(f"开始财务数据清洗任务 - {stock_info} | {periods_info} | {total_types}种数据类型")
        
        # 发送初始进度
        if self.progress_callback:
            self.progress_callback({
                "progress_percent": 0,
                "current_task": "准备开始处理财务数据",
                "processed_count": 0,
                "total_count": total_types,
                "current_type": "",
                "batch_info": f"{total_types}种数据类型 | {stock_info}",
            })
        
        # 逐个类型处理
        for idx, data_type in enumerate(data_types):
            try:
                if self.progress_callback:
                    self.progress_callback({
                        "progress_percent": int((idx / total_types) * 100),
                        "current_task": f"正在处理 {data_type} 数据",
                        "processed_count": idx,
                        "total_count": total_types,
                        "current_type": data_type,
                        "batch_info": f"{data_type} - {stock_info}",
                    })
                
                # 使用新的清洗方法
                if data_type == 'income':
                    result = self.cleaner.clean_financial_income(ts_symbols, periods, use_vip)
                elif data_type == 'balance':
                    result = self.cleaner.clean_financial_balance(ts_symbols, periods, use_vip)
                elif data_type == 'cashflow':
                    result = self.cleaner.clean_financial_cashflow(ts_symbols, periods, use_vip)
                elif data_type == 'indicator':
                    result = self.cleaner.clean_financial_indicator(ts_symbols, periods, use_vip)
                else:
                    logger.warning(f"❓ 未知的数据类型: {data_type}")
                    continue
                
            except Exception as e:
                logger.error(f"处理 {data_type} 数据失败: {str(e)}")
                
                if self.progress_callback:
                    self.progress_callback({
                        "current_task": f"处理 {data_type} 数据时出现错误",
                        "error_message": f"{data_type}失败: {str(e)[:100]}",
                    })
        
        # 发送完成状态
        if self.progress_callback:
            self.progress_callback({
                "progress_percent": 100,
                "current_task": "财务数据清洗已完成",
                "processed_count": total_types,
                "total_count": total_types,
                "current_type": "",
                "batch_info": f"完成 {total_types} 种数据类型",
                "status": "completed"
            })
        
        logger.info(f"财务数据清洗任务完成")
    
    def _format_periods_info(self, periods):
        """格式化报告期信息用于日志"""
        if periods is None:
            return "最新报告期"
        elif isinstance(periods, str):
            return f"报告期{periods}"
        elif isinstance(periods, list):
            if len(periods) == 1:
                return f"报告期{periods[0]}"
            else:
                return f"{len(periods)}个报告期"
        elif isinstance(periods, dict):
            return f"报告期{periods.get('start')}-{periods.get('end')}"
        else:
            return "未知报告期"
    
    def financial_daily_update(self, symbols=None, data_types=None):
        """
        每日财务数据更新（更新最近2个季度的数据）
        
        Args:
            symbols: 股票代码列表（pandas格式），如果为None则更新所有股票
            data_types: 数据类型列表，如果为None则更新所有类型
        """
        logger.info("📅 开始每日财务数据更新")
        
        # 获取最近2个季度
        recent_quarters = self.get_recent_quarters(num_quarters=2)
        
        if not recent_quarters:
            logger.warning("⚠️  未找到需要更新的季度")
            return
        
        logger.info(f"📋 更新季度: {', '.join(recent_quarters)}")
        
        # 使用最早和最晚的季度作为日期范围
        start_date = recent_quarters[-1]  # 最早的季度
        end_date = recent_quarters[0]      # 最晚的季度
        
        # 调用历史清洗方法
        self.financial_history_clean(start_date, end_date, symbols, data_types)
        
        logger.info("🎉 每日财务数据更新完成")

