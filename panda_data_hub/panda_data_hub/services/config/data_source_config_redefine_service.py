import os
from abc import ABC
import re
import yaml
from pathlib import Path
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.models.config_request import ConfigRequest
from panda_common import config as common_config
import importlib
import tushare as ts

modules_to_reload = [
    'panda_data_hub.routes.data_clean.factor_data_clean',
    'panda_data_hub.routes.data_clean.stock_market_data_clean',
    'panda_data_hub.task.data_scheduler',
    'panda_data_hub.task.factor_clean_scheduler'
]

class DataSourceConfigRedefine(ABC):

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)

    def data_source_config_redefine(self, requst: ConfigRequest):
        # 1. 找到配置文件路径
        config_path = Path(__file__).parent / '../../../../panda_common/panda_common/config.yaml'
        config_path = config_path.resolve()

        # 2. 读取文件内容
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        # 3. 定义字段更新规则（正则表达式模式）
        update_rules = {
            'ricequant': [
                (r'^(\s*)MUSER\s*:.*$', f'\\1MUSER: "{requst.m_user_name}"', requst.m_user_name),  # 添加条件字段
                (r'^(\s*)MPASSWORD\s*:.*$', f'\\1MPASSWORD: "{requst.m_password}"', requst.m_password),
                (r'^(\s*)DATAHUBSOURCE\s*:.*$', '\\1DATAHUBSOURCE: "ricequant"', True)  # 数据源必须更新
            ],
            'tushare': [
                (r'^(\s*)TS_TOKEN\s*:.*$', f'\\1TS_TOKEN: "{requst.admin_token}"', requst.admin_token),
                (r'^(\s*)DATAHUBSOURCE\s*:.*$', '\\1DATAHUBSOURCE: "tushare"', True)
            ],
            'xuntou': [
                (r'^(\s*)XT_TOKEN\s*:.*$', f'\\1XT_TOKEN: "{requst.admin_token}"', requst.admin_token),
                (r'^(\s*)DATAHUBSOURCE\s*:.*$', '\\1DATAHUBSOURCE: "xuntou"', True)
            ]
        }

        # MongoDB配置更新规则
        mongo_update_rules = [
            (r'^(\s*)MONGO_URI\s*:.*$', f'\\1MONGO_URI: "{requst.mongo_uri}"', requst.mongo_uri),
            (r'^(\s*)MONGO_USER\s*:.*$', f'\\1MONGO_USER: "{requst.username}"', requst.username),
            (r'^(\s*)MONGO_PASSWORD\s*:.*$', f'\\1MONGO_PASSWORD: "{requst.password}"', requst.password),
            (r'^(\s*)MONGO_AUTH_DB\s*:.*$', f'\\1MONGO_AUTH_DB: "{requst.auth_db}"', requst.auth_db),
            (r'^(\s*)MONGO_DB\s*:.*$', f'\\1MONGO_DB: "{requst.db_name}"', requst.db_name)
        ]

        # 数据清洗时间配置更新规则
        clean_time_update_rules = [
            (r'^(\s*)STOCKS_UPDATE_TIME\s*:.*$', f'\\1STOCKS_UPDATE_TIME: "{requst.stock_clean_time}"',
             requst.stock_clean_time),
            (r'^(\s*)FACTOR_UPDATE_TIME\s*:.*$', f'\\1FACTOR_UPDATE_TIME: "{requst.factor_clean_time}"',
             requst.factor_clean_time)
        ]

        # 4. 应用更新规则
        if requst.data_source not in update_rules:
            raise ValueError(f"不支持的数据源: {requst.data_source} (支持: {list(update_rules.keys())})")

        # 应用数据源更新规则
        for pattern, replacement, condition in update_rules[requst.data_source]:
            if condition != "":  # 只有当条件字段不为空字符串时才更新
                content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

        # 应用MongoDB配置更新规则
        for pattern, replacement, condition in mongo_update_rules:
            if condition != "":
                content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

        # 应用数据清洗时间配置更新规则
        for pattern, replacement, condition in clean_time_update_rules:
            if condition != "":
                content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

        # 5. 安全写入（原子操作）
        temp_path = config_path.with_suffix('.tmp')
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
            # 原子替换原文件
            os.replace(temp_path, config_path)
            logger.info("配置文件更新成功")
            
            # 6. 热加载配置到内存
            self._reload_config()
            
            # 7. 重新设置数据源token
            self._reload_data_source_token(requst)
            
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise RuntimeError(f"配置文件更新失败: {e}")
    
    def _reload_config(self):
        """重新加载配置到内存"""
        try:
            from panda_common.config import load_config
            new_config = load_config()
            
            # 更新当前config对象的所有键值
            common_config.config.clear()
            common_config.config.update(new_config)
            
            logger.info("配置已热加载到内存")
        except Exception as e:
            logger.error(f"配置热加载失败: {str(e)}")
            raise
    
    def _reload_data_source_token(self, request: ConfigRequest):
        """重新设置数据源token"""
        try:
            if request.data_source == 'tushare' and request.admin_token:
                # 重新设置tushare token
                ts.set_token(request.admin_token)
                logger.info(f"Tushare token已更新")
                
            elif request.data_source == 'ricequant' and request.m_user_name and request.m_password:
                # 重新登录ricequant
                try:
                    import rqdatac
                    rqdatac.init(request.m_user_name, request.m_password)
                    logger.info(f"RiceQuant已重新登录")
                except Exception as e:
                    logger.warning(f"RiceQuant重新登录失败: {str(e)}")
                    
            elif request.data_source == 'xuntou' and request.admin_token:
                # 讯投的token处理
                logger.info(f"XunTou token已更新")
                
        except Exception as e:
            logger.error(f"数据源token更新失败: {str(e)}")
            raise



    def get_datahub_resource(self):
        datahub_source = self.config.get("DATAHUBSOURCE")
        return datahub_source
