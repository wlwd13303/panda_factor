"""
增强版财务数据清洗器 - 支持单字段更新
功能：支持只更新指定的字段，节省API调用
作者：PandaAI
日期：2025-10-23
"""

from typing import List, Optional
from tqdm import tqdm
import time
from panda_common.logger_config import logger
from panda_data_hub.data.tushare_financial_cleaner import TSFinancialCleaner
from pymongo import UpdateOne


class FinancialDataCleanerTSEnhanced(TSFinancialCleaner):
    """
    增强版财务数据清洗器
    新增功能：支持只更新指定字段，避免浪费API
    """
    
    def clean_financial_indicator_partial(
        self,
        symbols: List[str],
        periods: List[str],
        fields_to_update: Optional[List[str]] = None,
        mode: str = 'partial'  # 'partial' 或 'full'
    ):
        """
        部分更新财务指标数据
        
        Args:
            symbols: 股票代码列表
            periods: 报告期列表
            fields_to_update: 要更新的字段列表，None表示更新所有字段
            mode: 'partial' 只更新指定字段，'full' 全量更新
        
        使用示例:
            # 只更新 q_roe 和 q_dt_roe 两个字段
            cleaner.clean_financial_indicator_partial(
                symbols=['000001.SZ', '000002.SZ'],
                periods=['20240930', '20240630'],
                fields_to_update=['q_roe', 'q_dt_roe'],
                mode='partial'
            )
        """
        from panda_common.utils.stock_utils import get_exchange_suffix
        from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes_financial
        
        # 基础字段（必须获取，用于唯一标识）
        # 注意：fina_indicator 接口不返回 report_type 字段
        base_fields = [
            'ts_code',      # 股票代码
            'ann_date',     # 公告日期
            'end_date',     # 报告期
            'update_flag'   # 更新标志
        ]
        
        # 如果指定了要更新的字段，只获取这些字段
        if fields_to_update and mode == 'partial':
            # 合并基础字段和要更新的字段
            fields_to_fetch = list(set(base_fields + fields_to_update))
            logger.info(f"部分更新模式：只更新字段 {fields_to_update}")
        else:
            # 获取所有字段
            fields_to_fetch = [
                'ts_code', 'ann_date', 'end_date', 'report_type', 'update_flag',
                'eps', 'dt_eps', 'total_revenue_ps', 'revenue_ps', 'capital_rese_ps',
                'surplus_rese_ps', 'undist_profit_ps', 'extra_item', 'profit_dedt',
                'gross_margin', 'current_ratio', 'quick_ratio', 'cash_ratio',
                'ar_turn', 'ca_turn', 'fa_turn', 'assets_turn', 'op_income',
                'ebit', 'ebitda', 'fcff', 'fcfe', 'current_exint',
                'noncurrent_exint', 'interestdebt', 'netdebt', 'tangible_asset',
                'working_capital', 'networking_capital', 'invest_capital', 'retained_earnings',
                'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps',
                'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 'grossprofit_margin',
                'cogs_of_sales', 'expense_of_sales', 'profit_to_gr', 'saleexp_to_gr',
                'adminexp_of_gr', 'finaexp_of_gr', 'impai_ttm', 'gc_of_gr',
                'op_of_gr', 'ebit_of_gr', 'roe', 'roe_waa',
                'roe_dt', 'roa', 'npta', 'roic',
                'roe_yearly', 'roa2_yearly', 'debt_to_assets', 'assets_to_eqt',
                'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets', 'tbassets_to_totalassets',
                'int_to_talcap', 'eqt_to_talcapital', 'currentdebt_to_debt', 'longdeb_to_debt',
                'ocf_to_shortdebt', 'debt_to_eqt', 'eqt_to_debt', 'eqt_to_interestdebt',
                'tangibleasset_to_debt', 'tangasset_to_intdebt', 'tangibleasset_to_netdebt', 'ocf_to_debt',
                'turn_days', 'roa_yearly', 'q_opincome', 'q_investincome',
                'q_dtprofit', 'q_eps', 'q_netprofit_margin', 'q_gsprofit_margin',
                'q_exp_to_sales', 'q_profit_to_gr', 'q_saleexp_to_gr', 'q_adminexp_to_gr',
                'q_finaexp_to_gr', 'q_impair_to_gr_ttm', 'q_gc_to_gr', 'q_op_to_gr',
                'q_roe', 'q_dt_roe', 'q_npta', 'q_ocf_to_sales',
                'basic_eps_yoy', 'dt_eps_yoy', 'cfps_yoy', 'op_yoy',
                'ebt_yoy', 'netprofit_yoy', 'dt_netprofit_yoy', 'ocf_yoy',
                'roe_yoy', 'bps_yoy', 'assets_yoy', 'eqt_yoy',
                'tr_yoy', 'or_yoy', 'q_sales_yoy', 'q_op_qoq',
                'equity_yoy', 'rd_exp', 'rd_exp_yoy'
            ]
            logger.info(f"全量更新模式：获取所有字段")
        
        total_tasks = len(symbols) * len(periods)
        processed = 0
        
        logger.info(f"开始清洗财务指标数据: {len(symbols)} 只股票 × {len(periods)} 个报告期 = {total_tasks} 个任务")
        
        with tqdm(total=total_tasks, desc=f"清洗财务指标数据 ({'部分' if mode == 'partial' else '全量'})") as pbar:
            for symbol in symbols:
                for period in periods:
                    try:
                        # 调用 Tushare API，只获取需要的字段
                        df = self.pro.fina_indicator(
                            ts_code=symbol,
                            period=period,
                            fields=','.join(fields_to_fetch)
                        )
                        
                        # 只保留初次公告数据
                        df = df[df['update_flag'] == '0']
                        
                        if not df.empty:
                            # 转换股票代码格式
                            df['ts_code'] = df['ts_code'].apply(get_exchange_suffix)
                            df = df.rename(columns={'ts_code': 'symbol'})
                            
                            # 确保集合和索引存在
                            ensure_collection_and_indexes_financial('financial_indicator')
                            
                            # 批量更新
                            upsert_operations = []
                            for _, row in df.iterrows():
                                record = row.to_dict()
                                
                                # 构建查询条件
                                query = {
                                    'symbol': record['symbol'],
                                    'end_date': record['end_date'],
                                    'ann_date': record['ann_date']
                                }
                                
                                if mode == 'partial' and fields_to_update:
                                    # 部分更新：只更新指定的字段
                                    update_data = {
                                        field: record.get(field)
                                        for field in fields_to_update
                                        if field in record
                                    }
                                    # 同时更新基础字段（确保数据一致性）
                                    update_data.update({
                                        'symbol': record['symbol'],
                                        'end_date': record['end_date'],
                                        'ann_date': record['ann_date'],
                                        'update_flag': record.get('update_flag')
                                    })
                                    
                                    # 如果 API 返回了 report_type，也更新它
                                    if 'report_type' in record and record.get('report_type') is not None:
                                        update_data['report_type'] = record['report_type']
                                else:
                                    # 全量更新：更新所有字段
                                    update_data = record
                                
                                upsert_operations.append(UpdateOne(
                                    query,
                                    {'$set': update_data},  # 只更新 update_data 中的字段
                                    upsert=True
                                ))
                            
                            if upsert_operations:
                                collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['financial_indicator']
                                result = collection.bulk_write(upsert_operations)
                                
                                logger.info(
                                    f"{'部分更新' if mode == 'partial' else '全量更新'} "
                                    f"{symbol} {period} 财务指标数据，"
                                    f"修改: {result.modified_count}, 插入: {result.upserted_count}"
                                )
                        
                        processed += 1
                        pbar.update(1)
                        
                        # API 限流
                        time.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"处理 {symbol} {period} 财务指标数据失败: {str(e)}")
                        processed += 1
                        pbar.update(1)
                        continue
        
        logger.info(f"财务指标数据清洗完成，处理了 {processed}/{total_tasks} 个任务")
        
        return {
            'total': total_tasks,
            'processed': processed,
            'mode': mode,
            'fields': fields_to_update if mode == 'partial' else 'all'
        }


# 使用示例
if __name__ == "__main__":
    from panda_common.config import config
    
    # 创建增强版清洗器
    cleaner = FinancialDataCleanerTSEnhanced(config)
    
    # 示例1：只更新 q_roe 和 q_dt_roe 两个字段
    print("=" * 70)
    print("示例1：部分更新 - 只更新 q_roe 和 q_dt_roe")
    print("=" * 70)
    
    result = cleaner.clean_financial_indicator_partial(
        symbols=['000001.SZ', '000002.SZ'],
        periods=['20240930', '20240630'],
        fields_to_update=['q_roe', 'q_dt_roe'],
        mode='partial'
    )
    
    print(f"\n更新结果: {result}")
    
    # 示例2：全量更新
    print("\n" + "=" * 70)
    print("示例2：全量更新 - 更新所有字段")
    print("=" * 70)
    
    result = cleaner.clean_financial_indicator_partial(
        symbols=['000001.SZ'],
        periods=['20240930'],
        fields_to_update=None,
        mode='full'
    )
    
    print(f"\n更新结果: {result}")

