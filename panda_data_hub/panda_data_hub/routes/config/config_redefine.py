from fastapi import APIRouter, HTTPException
from panda_common.config import get_config
from panda_data_hub.models.config_request import ConfigRequest
from panda_data_hub.services.config.data_source_config_redefine_service import DataSourceConfigRedefine
from panda_common.logger_config import logger

router = APIRouter()

@router.get('/get_current_config')
async def get_current_config():
    """获取当前配置信息"""
    try:
        # 动态获取最新配置
        current_config = get_config()
        return {
            "success": True,
            "data": {
                "mongodb": {
                    "uri": current_config.get("MONGO_URI", ""),
                    "username": current_config.get("MONGO_USER", ""),
                    "password": current_config.get("MONGO_PASSWORD", ""),
                    "auth_db": current_config.get("MONGO_AUTH_DB", "admin"),
                    "db_name": current_config.get("MONGO_DB", "panda"),
                    "type": current_config.get("MONGO_TYPE", "single"),
                    "replica_set": current_config.get("MONGO_REPLICA_SET", "")
                },
                "clean_time": {
                    "stock_update_time": current_config.get("STOCKS_UPDATE_TIME", "20:00"),
                    "factor_update_time": current_config.get("FACTOR_UPDATE_TIME", "21:00"),
                    "financial_update_time": current_config.get("FINANCIAL_UPDATE_TIME", "16:30")
                },
                "data_source": {
                    "current": current_config.get("DATAHUBSOURCE", "tushare"),
                    "ts_token": current_config.get("TS_TOKEN", "")
                }
            }
        }
    except Exception as e:
        logger.error(f"获取配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")


@router.post('/test_tushare_token')
async def test_tushare_token(request: dict):
    """测试Tushare Token是否有效"""
    try:
        from panda_data_hub.utils.ts_utils import validate_tushare_token
        import tushare as ts
        from datetime import datetime, timedelta
        
        token = request.get('token', '')
        if not token:
            return {
                "success": False,
                "message": "Token不能为空"
            }
        
        # 设置token并测试
        ts.set_token(token)
        pro = ts.pro_api()
        
        # 获取最近3天的指数行情作为测试
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # 考虑到周末，往前推7天确保有3个交易日
        
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        # 查询上证指数行情
        df = pro.index_daily(ts_code='000001.SH', start_date=start_str, end_date=end_str)
        
        if df is not None and not df.empty:
            # 只取最近3条记录
            recent_data = df.head(3)
            
            result_data = []
            for _, row in recent_data.iterrows():
                result_data.append({
                    "date": row['trade_date'],
                    "close": float(row['close']),
                    "pct_change": float(row['pct_chg'])
                })
            
            return {
                "success": True,
                "message": "Token验证成功！已获取上证指数最近3天行情数据",
                "data": result_data
            }
        else:
            return {
                "success": False,
                "message": "Token可能有效，但未能获取到数据"
            }
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Token测试失败: {error_msg}")
        
        if 'token' in error_msg.lower() or '不对' in error_msg or '权限' in error_msg:
            return {
                "success": False,
                "message": f"Token无效: {error_msg}"
            }
        else:
            return {
                "success": False,
                "message": f"测试失败: {error_msg}"
            }


@router.post('/config_redefine_data_source')
async def config_redefine_data_source(requst: ConfigRequest):
    """修改配置文件并热加载到内存"""
    try:
        # 动态获取最新配置
        current_config = get_config()
        service = DataSourceConfigRedefine(current_config)
        service.data_source_config_redefine(requst)
        return {
            "message": "配置文件修改成功并已热加载",
            "detail": "新配置已立即生效，无需重启项目",
            "success": True
        }
    except Exception as e:
        logger.error(f"配置更新失败: {str(e)}")
        return {
            "message": f"配置更新失败: {str(e)}",
            "success": False
        }


@router.get('/get_datahub_resource')
async def get_datahub_resource():
    """获取当前数据源"""
    try:
        # 动态获取最新配置
        current_config = get_config()
        service = DataSourceConfigRedefine(current_config)
        return {
            "success": True,
            "data_source": service.get_datahub_resource()
        }
    except Exception as e:
        logger.error(f"获取数据源失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取数据源失败: {str(e)}")

