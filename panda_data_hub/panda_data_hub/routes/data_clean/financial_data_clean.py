from typing import Dict, Optional, List
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import get_config
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
    
    # 动态获取最新配置
    current_config = get_config()
    
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
        "data_source": current_config.get('DATAHUBSOURCE', 'tushare'),
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
    data_source = current_config.get('DATAHUBSOURCE', 'tushare')
    
    if data_source == 'tushare':
        service = FinancialCleanTSService(current_config)
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
    
    # 动态获取最新配置
    current_config = get_config()
    
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
        "data_source": current_config.get('DATAHUBSOURCE', 'tushare'),
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
    data_source = current_config.get('DATAHUBSOURCE', 'tushare')
    
    if data_source == 'tushare':
        service = FinancialCleanTSService(current_config)
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


@router.post('/clean_financial_by_periods')
async def clean_financial_by_periods(
    background_tasks: BackgroundTasks,
    periods: Optional[str] = None,  # 报告期（单个或多个，逗号分隔）如："20240930" 或 "20240331,20240630"
    period_start: Optional[str] = None,  # 报告期范围开始
    period_end: Optional[str] = None,    # 报告期范围结束
    symbols: Optional[str] = None,  # 股票代码（逗号分隔）
    data_types: Optional[str] = None,  # 数据类型（逗号分隔）
    use_vip: Optional[bool] = None  # 是否使用VIP接口
):
    """
    按报告期清洗财务数据（支持VIP接口）
    
    Args:
        periods: 报告期（单个或多个，逗号分隔），如："20240930" 或 "20240331,20240630"
        period_start: 报告期范围开始（与period_end配合使用）
        period_end: 报告期范围结束（与period_start配合使用）
        symbols: 股票代码（逗号分隔），为空表示全市场
        data_types: 数据类型（逗号分隔），为空表示所有类型
        use_vip: 是否强制使用VIP接口，null表示自动判断
    
    示例:
        1. 最新报告期，全市场：
           POST /clean_financial_by_periods
           
        2. 指定单个报告期，指定股票：
           POST /clean_financial_by_periods?periods=20240930&symbols=000001.SZ,600519.SH
           
        3. 指定报告期范围，全市场：
           POST /clean_financial_by_periods?period_start=20240331&period_end=20240930
    """
    global financial_progress
    
    # 动态获取最新配置
    current_config = get_config()
    
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
        "data_source": current_config.get('DATAHUBSOURCE', 'tushare'),
        "batch_info": "",
    })
    
    # 解析报告期参数
    periods_param = None
    if periods:
        # 单个或多个报告期
        periods_list = [p.strip() for p in periods.split(',') if p.strip()]
        periods_param = periods_list[0] if len(periods_list) == 1 else periods_list
    elif period_start and period_end:
        # 报告期范围
        periods_param = {"start": period_start, "end": period_end}
    # else: periods_param 为 None，表示使用最新报告期
    
    # 解析股票参数
    symbols_list = None
    if symbols:
        symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
    
    # 解析数据类型
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
    data_source = current_config.get('DATAHUBSOURCE', 'tushare')
    
    if data_source == 'tushare':
        service = FinancialCleanTSService(current_config)
        service.set_progress_callback(progress_callback)
        
        # 在后台运行清洗任务
        background_tasks.add_task(
            service.clean_financial_by_periods,
            symbols_list,
            periods_param,
            data_types_list,
            use_vip
        )
    else:
        return {
            "error": f"不支持的数据源: {data_source}",
            "message": "当前仅支持 tushare 数据源"
        }
    
    # 构建返回信息
    symbols_info = "全市场" if not symbols_list else f"{len(symbols_list)} 只股票"
    types_info = "所有类型" if not data_types_list else f"{len(data_types_list)} 种类型"
    
    if periods_param is None:
        periods_info = "最新报告期"
    elif isinstance(periods_param, str):
        periods_info = f"报告期 {periods_param}"
    elif isinstance(periods_param, list):
        periods_info = f"{len(periods_param)} 个报告期"
    elif isinstance(periods_param, dict):
        periods_info = f"报告期范围 {periods_param['start']} - {periods_param['end']}"
    else:
        periods_info = "未知"
    
    vip_info = ""
    if use_vip is True:
        vip_info = "（强制使用VIP接口）"
    elif use_vip is False:
        vip_info = "（使用普通接口）"
    else:
        vip_info = "（自动选择接口）"
    
    return {
        "message": f"财务数据清洗任务已启动{vip_info}",
        "data_source": data_source,
        "periods": periods_info,
        "symbols": symbols_info,
        "data_types": types_info
    }


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
    
    # 动态获取最新配置
    current_config = get_config()
    
    # 使用配置文件中的日期范围
    start_date = str(current_config.get('HUB_START_DATE', '20170101'))
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
        "data_source": current_config.get('DATAHUBSOURCE', 'tushare'),
        "batch_info": "测试模式 - 5只股票",
    })
    
    # 解析股票代码
    symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
    
    def progress_callback(progress_info: dict):
        """进度回调函数"""
        global financial_progress
        financial_progress.update(progress_info)
    
    # 创建服务实例
    service = FinancialCleanTSService(current_config)
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

