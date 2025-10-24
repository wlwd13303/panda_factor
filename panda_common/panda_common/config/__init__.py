"""
配置模块

包含：
- financial_fields: 财务字段配置（集中管理所有财务报表字段）
"""

from panda_common.config.financial_fields import (
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

