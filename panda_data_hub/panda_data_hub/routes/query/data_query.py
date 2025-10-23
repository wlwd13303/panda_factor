from fastapi import APIRouter, Query

from panda_common.config import config
from panda_data_hub.services.query.stock_statistic_service import StockStatisticQuery

router = APIRouter()

@router.get('/data_query')
async def data_query(tables_name : str ,
                     start_date : str ,
                     end_date : str,
                     page: int = Query(default=1, ge=1, description="页码"),
                     page_size: int = Query(default=10, ge=1, le=100, description="每页数量"),
                     sort_field: str = Query(default="created_at", description="排序字段，支持created_at、return_ratio、sharpe_ratio、maximum_drawdown、IC、IR"),
                     sort_order: str = Query(default="desc", description="排序方式，asc升序，desc降序")
                     ):
    """ 根据表名和起止时间获取统计数据 """
    service = StockStatisticQuery(config)
    result_data = service.get_stock_statistic(tables_name,start_date,end_date,page, page_size,sort_field, sort_order)

    return result_data

@router.get('/get_trading_days')
async def get_trading_days(
        start_date: str ,
        end_date: str ,
):
    service = StockStatisticQuery(config)
    result_data = service.get_trading_days(start_date,end_date)
    return result_data

@router.get('/count_collection')
async def count_collection(collection: str):
    """查询指定集合的记录总数"""
    from panda_common.handlers.database_handler import DatabaseHandler
    
    # 允许查询的集合列表（白名单）
    allowed_collections = [
        'stock_market',
        'factor_base',
        'financial_indicator',
        'financial_income',
        'financial_balance',
        'financial_cashflow'
    ]
    
    if collection not in allowed_collections:
        return {"error": "不允许查询的集合", "count": 0}
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            collection
        )
        count = mongo_collection.count_documents({})
        return {"collection": collection, "count": count}
    except Exception as e:
        return {"error": str(e), "count": 0}

@router.get('/financial_stats_by_quarter')
async def financial_stats_by_quarter():
    """按季度统计财务数据（最近4个季度）"""
    from panda_common.handlers.database_handler import DatabaseHandler
    from datetime import datetime
    
    try:
        db_handler = DatabaseHandler(config)
        
        # 财务数据表列表
        financial_tables = {
            'financial_indicator': '财务指标',
            'financial_income': '利润表',
            'financial_balance': '资产负债表',
            'financial_cashflow': '现金流量表'
        }
        
        # 生成最近4个季度（从当前往前推）
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # 确定当前季度
        if current_month <= 3:
            current_quarter = 1
        elif current_month <= 6:
            current_quarter = 2
        elif current_month <= 9:
            current_quarter = 3
        else:
            current_quarter = 4
        
        # 生成最近4个季度的报告期（YYYYMMDD格式，季度末日期）
        quarters = []
        year = current_year
        quarter = current_quarter
        
        for i in range(4):
            if quarter == 1:
                end_date = f"{year}0331"
            elif quarter == 2:
                end_date = f"{year}0630"
            elif quarter == 3:
                end_date = f"{year}0930"
            else:
                end_date = f"{year}1231"
            
            quarters.append({
                'end_date': end_date,
                'label': f"{year}Q{quarter}"
            })
            
            # 往前推一个季度
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1
        
        # 查询每个表在每个季度的数据量
        result = []
        for table_name, table_label in financial_tables.items():
            mongo_collection = db_handler.get_mongo_collection(
                config["MONGO_DB"],
                table_name
            )
            
            quarter_stats = []
            total_count = 0
            
            for quarter_info in quarters:
                count = mongo_collection.count_documents({
                    "end_date": quarter_info['end_date']
                })
                quarter_stats.append({
                    'quarter': quarter_info['label'],
                    'end_date': quarter_info['end_date'],
                    'count': count
                })
                total_count += count
            
            result.append({
                'table': table_name,
                'label': table_label,
                'quarters': quarter_stats,
                'total': total_count
            })
        
        return {
            'success': True,
            'data': result,
            'quarters': [q['label'] for q in quarters]
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': []
        }




