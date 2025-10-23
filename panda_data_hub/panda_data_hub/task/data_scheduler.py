from panda_common.config import config, logger
from panda_common.handlers.database_handler import DatabaseHandler


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import datetime

from panda_data_hub.data.ricequant_stock_market_cleaner import RQStockMarketCleaner
from panda_data_hub.data.ricequant_stocks_cleaner import RQStockCleaner
from panda_data_hub.data.tushare_stocks_cleaner import TSStockCleaner
from panda_data_hub.data.tushare_stock_market_cleaner import TSStockMarketCleaner
from panda_data_hub.services.ts_financial_clean_service import FinancialCleanTSService
# from panda_data_hub.data.xtquant_stock_market_cleaner import XTStockMarketCleaner
# from panda_data_hub.data.xtquant_stocks_cleaner import XTStockCleaner


class DataScheduler:
    def __init__(self):
        self.config = config
        
        # Initialize database connection
        self.db_handler = DatabaseHandler(self.config)
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def _process_data(self):
        """处理数据清洗和入库"""
        logger.info(f"Processing data ")
        try:
            data_source = config['DATAHUBSOURCE']
            if data_source == 'ricequant':
                # 清洗stock表当日数据
                stocks_cleaner = RQStockCleaner(self.config)
                stocks_cleaner.clean_metadata()
                # 清洗stock_market表当日数据
                stock_market_cleaner = RQStockMarketCleaner(self.config)
                stock_market_cleaner.stock_market_clean_daily()
            elif data_source == 'tushare':
                # 清洗stock表当日数据
                stocks_cleaner = TSStockCleaner(self.config)
                stocks_cleaner.clean_metadata()
                # 清洗stock_market表当日数据
                stock_market_cleaner = TSStockMarketCleaner(self.config)
                stock_market_cleaner.stock_market_clean_daily()
            # elif data_source == 'xuntou':
            #     # 清洗stock表当日数据
            #     stocks_cleaner = XTStockCleaner(self.config)
            #     stocks_cleaner.clean_metadata()
            #     # 清洗stock_market表当日数据
            #     stock_market_cleaner = XTStockMarketCleaner(self.config)
            #     stock_market_cleaner.stock_market_clean_daily()
        except Exception as e:
            logger.error(f"Error _process_data : {str(e)}")
    
    def _process_financial_data(self):
        """处理财务数据清洗和入库（每日更新最近2个季度）"""
        logger.info("Processing financial data")
        try:
            data_source = config['DATAHUBSOURCE']
            if data_source == 'tushare':
                # 每日更新财务数据（最近2个季度）
                financial_service = FinancialCleanTSService(self.config)
                financial_service.financial_daily_update()
                logger.info("Financial data daily update completed")
            else:
                logger.warning(f"Financial data update not supported for data source: {data_source}")
        except Exception as e:
            logger.error(f"Error _process_financial_data: {str(e)}")
    
    def schedule_data(self):
        time = self.config["STOCKS_UPDATE_TIME"]
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
                
        # Add scheduled task
        self.scheduler.add_job(
            self._process_data,
            trigger=trigger,
            id=f"data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        # self._process_data()
        logger.info(f"Scheduled Data")
    
    def schedule_financial_data(self):
        """调度财务数据更新任务"""
        time = self.config.get("FINANCIAL_UPDATE_TIME", "16:30")
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
        
        # Add scheduled task for financial data
        self.scheduler.add_job(
            self._process_financial_data,
            trigger=trigger,
            id=f"financial_data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        logger.info(f"Scheduled Financial Data update at {time}")
    
    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown() 

    def reload_schedule(self):
        """重新加载定时任务（用于配置变更后热更新）"""
        self.scheduler.remove_all_jobs()
        self.schedule_data()
        self.schedule_financial_data()