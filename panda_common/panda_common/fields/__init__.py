"""
配置模块

包含：
- financial_fields: 财务字段配置（集中管理所有财务报表字段）
- get_config: 获取全局配置
"""

# Import config functions from the parent-level config.py module
# Using relative import to avoid naming conflict with this config package
from .. import config as _config_module
get_config = _config_module.get_config
load_config = _config_module.load_config

from panda_common.fields.financial_fields import (
    ALL_FINANCIAL_FIELDS,
    INCOME_FIELDS,
    BALANCE_FIELDS,
    CASHFLOW_FIELDS,
    INDICATOR_FIELDS,
    FINANCIAL_FIELDS_BY_TYPE,
    COLLECTION_MAPPING,
    get_financial_fields,
    is_financial_field,
    get_collection_name,
)

__all__ = [
    'get_config',
    'load_config',
    'ALL_FINANCIAL_FIELDS',
    'INCOME_FIELDS',
    'BALANCE_FIELDS',
    'CASHFLOW_FIELDS',
    'INDICATOR_FIELDS',
    'FINANCIAL_FIELDS_BY_TYPE',
    'COLLECTION_MAPPING',
    'get_financial_fields',
    'is_financial_field',
    'get_collection_name',
]

