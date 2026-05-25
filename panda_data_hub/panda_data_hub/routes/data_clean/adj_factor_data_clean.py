from typing import Dict, Optional
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import get_config
from panda_common.logger_config import logger
from panda_data_hub.factor.ts_adj_factor_clean_pro import TSAdjFactorCleaner

router = APIRouter()

current_progress = {
    "progress_percent": 0,
    "status": "idle",
    "current_task": "",
    "trading_days_processed": 0,
    "trading_days_total": 0,
    "current_date": "",
    "stock_processed": 0,
    "stock_total": 0,
    "error_message": "",
    "data_source": "tushare",
    "batch_info": "",
}


@router.get("/upsert_adj_factor_final")
async def upsert_adj_factor_final(
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    force_update: bool = False,
):
    """启动复权因子历史数据清洗任务"""
    global current_progress

    logger.info(
        f"Received adj_factor request: start_date={start_date}, end_date={end_date}, force_update={force_update}"
    )

    current_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "正在初始化复权因子清洗任务...",
        "trading_days_processed": 0,
        "trading_days_total": 0,
        "current_date": "",
        "stock_processed": 0,
        "stock_total": 0,
        "error_message": "",
        "data_source": "tushare",
        "batch_info": "",
    })

    def progress_callback(progress_info: dict):
        global current_progress
        for key, value in progress_info.items():
            current_progress[key] = value

    def run_with_error_handling():
        global current_progress
        try:
            current_config = get_config()
            cleaner = TSAdjFactorCleaner(current_config)
            cleaner.set_progress_callback(progress_callback)
            cleaner.clean_history_adj_factor(start_date, end_date, force_update)
        except Exception as e:
            error_msg = f"复权因子清洗任务失败: {str(e)}"
            logger.error(error_msg)
            current_progress.update({
                "status": "error",
                "error_message": error_msg,
                "current_task": "任务失败"
            })

    background_tasks.add_task(run_with_error_handling)
    return {"message": "复权因子数据清洗任务已启动", "success": True}


@router.get("/get_progress_adj_factor_final")
async def get_progress_adj_factor_final() -> Dict:
    """获取复权因子清洗任务进度"""
    try:
        global current_progress

        if (current_progress.get("progress_percent", 0) >= 100 and
                current_progress.get("status") == "running"):
            current_progress["status"] = "completed"
            current_progress["current_task"] = "任务完成"

        return current_progress
    except Exception as e:
        error_msg = f"获取进度失败: {str(e)}"
        logger.error(error_msg)
        return {
            "progress_percent": 0,
            "status": "error",
            "current_task": "获取进度失败",
            "error_message": error_msg,
            "trading_days_processed": 0,
            "trading_days_total": 0,
            "current_date": "",
            "stock_processed": 0,
            "stock_total": 0,
            "data_source": "tushare",
            "batch_info": "",
        }
