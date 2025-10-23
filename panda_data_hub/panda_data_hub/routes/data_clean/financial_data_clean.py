from typing import Dict, Optional, List
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import config
from panda_data_hub.services.ts_financial_clean_service import FinancialCleanTSService

router = APIRouter()

# 全局进度状态对象
financial_progress = {
    "progress_percent": 0,
    "status": "idle",  # idle, running, completed, error
    "current_task": "",
    "processed_count": 0,
    "total_count": 0,
    "start_time": None,
    "estimated_completion": None,
    "current_type": "",  # income, balance, cashflow, indicator
    "error_message": "",
    "data_source": "",
    "batch_info": "",
}


@router.get('/clean_financial_history')
async def clean_financial_history(
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    symbols: Optional[str] = None,  # 逗号分隔的股票代码，如："000001.SZ,600000.SH"
    data_types: Optional[str] = None  # 逗号分隔的数据类型，如："income,balance"
):
    """
    历史财务数据清洗接口
    
    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        symbols: 可选，指定股票代码（逗号分隔），为空则处理所有股票
        data_types: 可选，指定数据类型（逗号分隔），默认处理所有类型
                   可选值：income, balance, cashflow, indicator
    
    示例:
        /clean_financial_history?start_date=20170101&end_date=20231231&symbols=000001.SZ,600000.SH&data_types=income,indicator
    """
    global financial_progress
    
    # 重置进度状态
    financial_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化财务数据清洗任务...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_type": "",
        "error_message": "",
        "data_source": config.get('DATAHUBSOURCE', 'tushare'),
        "batch_info": "",
    })
    
    # 解析参数
    symbols_list = None
    if symbols:
        symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
    
    data_types_list = None
    if data_types:
        data_types_list = [dt.strip() for dt in data_types.split(',') if dt.strip()]
    
    def progress_callback(progress_info: dict):
        """进度回调函数"""
        global financial_progress
        financial_progress.update(progress_info)
        
        # 计算预计完成时间
        if (progress_info.get("progress_percent", 0) > 0 and 
            financial_progress.get("start_time")):
            try:
                start = datetime.fromisoformat(financial_progress["start_time"])
                elapsed = (datetime.now() - start).total_seconds()
                if progress_info["progress_percent"] > 0:
                    total_estimated = elapsed / (progress_info["progress_percent"] / 100)
                    remaining = total_estimated - elapsed
                    completion_time = datetime.now().timestamp() + remaining
                    financial_progress["estimated_completion"] = datetime.fromtimestamp(completion_time).isoformat()
            except Exception:
                pass
    
    # 创建服务实例
    data_source = config.get('DATAHUBSOURCE', 'tushare')
    
    if data_source == 'tushare':
        service = FinancialCleanTSService(config)
        service.set_progress_callback(progress_callback)
        
        # 在后台运行清洗任务
        background_tasks.add_task(
            service.financial_history_clean,
            start_date,
            end_date,
            symbols_list,
            data_types_list
        )
    else:
        return {
            "error": f"不支持的数据源: {data_source}",
            "message": "当前仅支持 tushare 数据源"
        }
    
    symbols_info = f"{len(symbols_list)} 只股票" if symbols_list else "所有股票"
    types_info = f"{len(data_types_list)} 种数据类型" if data_types_list else "所有数据类型"
    
    return {
        "message": f"财务数据清洗任务已启动",
        "data_source": data_source,
        "date_range": f"{start_date} - {end_date}",
        "symbols": symbols_info,
        "data_types": types_info
    }


@router.get('/update_financial_daily')
async def update_financial_daily(
    background_tasks: BackgroundTasks,
    symbols: Optional[str] = None,
    data_types: Optional[str] = None
):
    """
    每日财务数据更新接口（更新最近2个季度）
    
    Args:
        symbols: 可选，指定股票代码（逗号分隔）
        data_types: 可选，指定数据类型（逗号分隔）
    """
    global financial_progress
    
    # 重置进度状态
    financial_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化每日财务数据更新...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_type": "",
        "error_message": "",
        "data_source": config.get('DATAHUBSOURCE', 'tushare'),
        "batch_info": "更新最近2个季度的数据",
    })
    
    # 解析参数
    symbols_list = None
    if symbols:
        symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
    
    data_types_list = None
    if data_types:
        data_types_list = [dt.strip() for dt in data_types.split(',') if dt.strip()]
    
    def progress_callback(progress_info: dict):
        """进度回调函数"""
        global financial_progress
        financial_progress.update(progress_info)
    
    # 创建服务实例
    data_source = config.get('DATAHUBSOURCE', 'tushare')
    
    if data_source == 'tushare':
        service = FinancialCleanTSService(config)
        service.set_progress_callback(progress_callback)
        
        # 在后台运行更新任务
        background_tasks.add_task(
            service.financial_daily_update,
            symbols_list,
            data_types_list
        )
    else:
        return {
            "error": f"不支持的数据源: {data_source}",
            "message": "当前仅支持 tushare 数据源"
        }
    
    return {
        "message": "每日财务数据更新任务已启动",
        "data_source": data_source,
        "update_scope": "最近2个季度"
    }


@router.get('/get_financial_progress')
async def get_financial_progress() -> Dict:
    """获取财务数据清洗进度"""
    global financial_progress
    
    # 如果任务已完成，更新状态
    if (financial_progress.get("progress_percent", 0) >= 100 and 
        financial_progress.get("status") == "running"):
        financial_progress["status"] = "completed"
        financial_progress["current_task"] = "财务数据清洗已完成"
    
    return financial_progress


@router.get('/test_financial_clean')
async def test_financial_clean(
    background_tasks: BackgroundTasks,
    symbols: str = "000001.SZ,000002.SZ,000333.SZ,600000.SH,600519.SH"
):
    """
    测试接口：使用5只股票测试财务数据清洗流程
    
    Args:
        symbols: 股票代码，默认5只股票
    """
    global financial_progress
    
    # 使用配置文件中的日期范围
    start_date = str(config.get('HUB_START_DATE', '20170101'))
    end_date = datetime.now().strftime("%Y%m%d")
    
    # 重置进度状态
    financial_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "测试财务数据清洗（5只股票）...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_type": "",
        "error_message": "",
        "data_source": config.get('DATAHUBSOURCE', 'tushare'),
        "batch_info": "测试模式 - 5只股票",
    })
    
    # 解析股票代码
    symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
    
    def progress_callback(progress_info: dict):
        """进度回调函数"""
        global financial_progress
        financial_progress.update(progress_info)
    
    # 创建服务实例
    service = FinancialCleanTSService(config)
    service.set_progress_callback(progress_callback)
    
    # 在后台运行测试任务
    background_tasks.add_task(
        service.financial_history_clean,
        start_date,
        end_date,
        symbols_list,
        None  # 所有数据类型
    )
    
    return {
        "message": "测试财务数据清洗任务已启动",
        "data_source": "tushare",
        "date_range": f"{start_date} - {end_date}",
        "test_symbols": symbols_list,
        "data_types": "所有类型（income, balance, cashflow, indicator）"
    }

