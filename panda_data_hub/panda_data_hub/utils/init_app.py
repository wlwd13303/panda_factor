"""
应用初始化模块
在应用启动时初始化全局资源（如 tushare 连接）
每个进程只调用一次
"""
from panda_common.logger_config import logger
from panda_common.config import get_config


def init_app():
    """
    初始化应用全局资源
    应该在应用启动时调用一次
    """
    try:
        logger.info("开始初始化应用...")
        
        # 获取配置（此处仅确保配置可用，不主动连接 Tushare）
        config = get_config()
        
        logger.info("应用初始化完成")
        
    except Exception as e:
        logger.error(f"应用初始化失败: {str(e)}")
        raise
