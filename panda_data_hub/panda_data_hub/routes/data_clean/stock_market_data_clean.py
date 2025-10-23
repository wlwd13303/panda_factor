from typing import Dict
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import config
from panda_common.logger_config import logger

from panda_data_hub.services.rq_stock_market_clean_service import StockMarketCleanRQServicePRO
from panda_data_hub.services.ts_stock_market_clean_service import StockMarketCleanTSServicePRO
# from panda_data_hub.services.xt_download_service import XTDownloadService

# from panda_data_hub.services.xt_stock_market_clean_service import StockMarketCleanXTServicePRO

router = APIRouter()

# 全局进度状态对象，包含详细信息
current_progress = {
    "progress_percent": 0,              # 进度百分比
    "status": "idle",                   # 状态: idle, running, completed, error
    "current_task": "",                 # 当前处理的任务描述
    "processed_count": 0,               # 已处理数量
    "total_count": 0,                   # 总数量
    "start_time": None,                 # 开始时间
    "estimated_completion": None,       # 预计完成时间
    "current_date": "",                 # 当前处理的日期
    "error_message": "",                # 错误信息
    "data_source": "",                  # 数据源
    "batch_info": "",                   # 批次信息
    "trading_days_processed": 0,        # 已处理交易日数
    "trading_days_total": 0,            # 总交易日数
    # 细粒度（股票级）进度信息
    "stock_progress_percent": 0,        # 当前交易日内的股票级进度百分比
    "stock_phase": "",                  # index_component/name_clean/db_write
    "stock_processed": 0,               # 当前交易日内已处理股票数
    "stock_total": 0,                   # 当前交易日内总股票数
    "db_write_count": 0,                # 写入数据库的股票数量（当前交易日）
    "last_message": ""                  # 最近一条可读的进度信息
}

@router.get('/upsert_stockmarket_final')
async def upsert_stockmarket(start_date: str, end_date: str, background_tasks: BackgroundTasks):
    global current_progress
    
    logger.info(f"收到数据清洗请求: start_date={start_date}, end_date={end_date}")
    
    # 重置进度状态
    current_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化数据清洗任务...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_date": "",
        "error_message": "",
        "data_source": config['DATAHUBSOURCE'],
        "batch_info": "",
        "trading_days_processed": 0,
        "trading_days_total": 0,
    })

    data_source = config['DATAHUBSOURCE']
    logger.info(f"使用数据源: {data_source}")

    def progress_callback(progress_info: dict):
        """
        增强的进度回调函数，接收详细的进度信息
        progress_info 包含:
        - progress_percent: 百分比
        - current_task: 当前任务描述 
        - current_date: 当前处理日期
        - processed_count: 已处理数量
        - total_count: 总数量
        - batch_info: 批次信息
        """
        global current_progress
        # 只更新提供的字段，保留其他字段
        for key, value in progress_info.items():
            current_progress[key] = value
        
        logger.debug(f"进度更新: {current_progress.get('progress_percent', 0)}% - {current_progress.get('current_task', '')}")
        
        # 计算预计完成时间
        if (progress_info.get("progress_percent", 0) > 0 and 
            current_progress.get("start_time")):
            try:
                start = datetime.fromisoformat(current_progress["start_time"])
                elapsed = (datetime.now() - start).total_seconds()
                if progress_info["progress_percent"] > 0:
                    total_estimated = elapsed / (progress_info["progress_percent"] / 100)
                    remaining = total_estimated - elapsed
                    completion_time = datetime.now().timestamp() + remaining
                    current_progress["estimated_completion"] = datetime.fromtimestamp(completion_time).isoformat()
            except Exception:
                pass  # 忽略时间计算错误

    def run_with_error_handling(task_func, *args):
        """包装任务执行，添加错误处理"""
        global current_progress
        try:
            task_func(*args)
        except Exception as e:
            error_msg = f"数据清洗出错: {str(e)}"
            logger.error(error_msg)
            current_progress.update({
                "status": "error",
                "error_message": error_msg,
                "current_task": "数据清洗失败"
            })
    
    if data_source  == 'ricequant':
        logger.info("初始化RiceQuant服务")
        rice_quant_service = StockMarketCleanRQServicePRO(config)
        rice_quant_service.set_progress_callback(progress_callback)
        # 在后台运行数据清洗任务
        background_tasks.add_task(
            run_with_error_handling,
            rice_quant_service.stock_market_clean_by_time,
            start_date,
            end_date
        )
        logger.info("RiceQuant后台任务已添加")
    elif data_source == 'tushare':
        logger.info("初始化Tushare服务")
        tushare_service = StockMarketCleanTSServicePRO(config)
        tushare_service.set_progress_callback(progress_callback)
        background_tasks.add_task(
            run_with_error_handling,
            tushare_service.stock_market_history_clean,
            start_date,
            end_date
        )
        logger.info("Tushare后台任务已添加")
    # elif data_source == 'xuntou':
    #     xt_quant_service = StockMarketCleanXTServicePRO(config)
    #     xt_quant_service.set_progress_callback(progress_callback)
    #     background_tasks.add_task(
    #         run_with_error_handling,
    #         xt_quant_service.stock_market_history_clean,
    #         start_date,
    #         end_date
    #     )
    return {"message": f"Stock market data cleaning started by {data_source}"}


@router.get('/get_progress_stock_final')
async def get_progress() -> Dict:
    """获取当前数据清洗进度 - 返回详细的进度信息"""
    try:
        global current_progress
        logger.info("收到进度查询请求")
        
        # 如果任务已完成，更新状态
        if (current_progress.get("progress_percent", 0) >= 100 and 
            current_progress.get("status") == "running"):
            current_progress["status"] = "completed"
            current_progress["current_task"] = "数据清洗已完成"
        
        logger.info(f"返回进度信息: status={current_progress.get('status')}, percent={current_progress.get('progress_percent')}%, task={current_progress.get('current_task')}")
        return current_progress
    except Exception as e:
        error_msg = f"获取进度信息失败: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return {
            "progress_percent": 0,
            "status": "error",
            "current_task": "获取进度失败",
            "error_message": error_msg,
            "processed_count": 0,
            "total_count": 0
        }


# @router.get("/download_xt_data")
# async def download_xt_data(start_date: str, end_date: str,background_tasks: BackgroundTasks):
#
#     def progress_callback(progress: int):
#         global current_progress
#         current_progress = progress
#
#     service = XTDownloadService(config)
#     service.set_progress_callback(progress_callback)
#     background_tasks.add_task(
#         service.xt_price_data_download,
#         start_date,
#         end_date
#     )
#     return {"message": "XTData data downloading started"}
