"""
财务数据功能测试脚本

演示如何使用财务数据读取器获取和分析财务数据
"""

from panda_common.config import config
from panda_data.financial import FinancialDataReader


def test_basic_query():
    """测试基础查询功能"""
    print("\n=== 测试1: 基础查询功能 ===")
    
    reader = FinancialDataReader(config)
    
    # 查询指定股票的财务指标
    df = reader.get_financial_data(
        symbols=['000001.SZ', '600000.SH'],
        start_date='20230101',
        end_date='20231231',
        fields=['roe', 'roa', 'gross_margin', 'netprofit_margin'],
        data_type='indicator',
        date_type='ann_date'
    )
    
    if df is not None:
        print(f"查询到 {len(df)} 条数据")
        print("\n数据示例：")
        print(df.head())
        print("\n字段信息：")
        print(df.columns.tolist())
    else:
        print("未查询到数据")


def test_latest_financial():
    """测试获取最新财务数据"""
    print("\n=== 测试2: 获取最新财务数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 获取最新的财务指标
    latest_df = reader.get_latest_financial_data(
        symbols=['000001.SZ', '600000.SH', '600519.SH'],
        fields=['roe', 'roa', 'debt_to_assets'],
        data_type='indicator'
    )
    
    if latest_df is not None:
        print(f"查询到 {len(latest_df)} 只股票的最新数据")
        print("\n最新财务数据：")
        print(latest_df[['symbol', 'end_date', 'ann_date', 'roe', 'roa', 'debt_to_assets']])
    else:
        print("未查询到数据")


def test_quarterly_data():
    """测试按季度查询数据"""
    print("\n=== 测试3: 按季度查询数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 查询特定季度的利润表数据
    quarterly_df = reader.get_financial_data_by_quarter(
        symbols=['000001.SZ'],
        quarters=['20231231', '20230930', '20230630', '20230331'],
        fields=['revenue', 'n_income', 'operate_profit'],
        data_type='income'
    )
    
    if quarterly_df is not None:
        print(f"查询到 {len(quarterly_df)} 条季度数据")
        print("\n季度财务数据：")
        print(quarterly_df[['symbol', 'end_date', 'ann_date', 'revenue', 'n_income', 'operate_profit']])
    else:
        print("未查询到数据")


def test_time_series():
    """测试时间序列数据"""
    print("\n=== 测试4: 时间序列数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 获取单只股票的ROE时间序列
    ts_df = reader.get_financial_time_series(
        symbol='000001.SZ',
        fields=['roe', 'roa'],
        data_type='indicator',
        start_date='20200101',
        end_date='20231231',
        date_type='end_date'
    )
    
    if ts_df is not None:
        print(f"查询到 {len(ts_df)} 个时间点的数据")
        print("\nROE时间序列：")
        print(ts_df)
    else:
        print("未查询到数据")


def test_cross_section():
    """测试横截面数据"""
    print("\n=== 测试5: 横截面数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 获取2023年底所有股票的财务指标
    cross_df = reader.get_financial_cross_section(
        date='20231231',
        fields=['roe', 'roa', 'gross_margin'],
        data_type='indicator',
        date_type='end_date',
        symbols=['000001.SZ', '000002.SZ', '600000.SH', '600519.SH']
    )
    
    if cross_df is not None:
        print(f"查询到 {len(cross_df)} 只股票的横截面数据")
        print("\n横截面财务数据：")
        print(cross_df[['symbol', 'end_date', 'roe', 'roa', 'gross_margin']])
        
        # 简单分析
        print("\n统计信息：")
        print(cross_df[['roe', 'roa', 'gross_margin']].describe())
    else:
        print("未查询到数据")


def test_balance_sheet():
    """测试资产负债表数据"""
    print("\n=== 测试6: 资产负债表数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 查询资产负债表
    balance_df = reader.get_financial_data(
        symbols=['000001.SZ'],
        start_date='20230101',
        end_date='20231231',
        fields=['total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int'],
        data_type='balance',
        date_type='end_date'
    )
    
    if balance_df is not None:
        print(f"查询到 {len(balance_df)} 条资产负债表数据")
        print("\n资产负债表：")
        print(balance_df[['symbol', 'end_date', 'total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int']])
        
        # 验证资产负债表等式
        if len(balance_df) > 0:
            row = balance_df.iloc[0]
            assets = row['total_assets']
            liab = row['total_liab']
            equity = row['total_hldr_eqy_exc_min_int']
            print(f"\n验证: 资产({assets}) = 负债({liab}) + 权益({equity})")
            print(f"差异: {assets - (liab + equity)}")
    else:
        print("未查询到数据")


def test_cashflow():
    """测试现金流量表数据"""
    print("\n=== 测试7: 现金流量表数据 ===")
    
    reader = FinancialDataReader(config)
    
    # 查询现金流量表
    cashflow_df = reader.get_financial_data(
        symbols=['000001.SZ'],
        start_date='20230101',
        end_date='20231231',
        fields=['n_cashflow_act', 'n_cashflow_inv_act', 'n_cashflow_fnc_act'],
        data_type='cashflow',
        date_type='end_date'
    )
    
    if cashflow_df is not None:
        print(f"查询到 {len(cashflow_df)} 条现金流量表数据")
        print("\n现金流量表：")
        print(cashflow_df[['symbol', 'end_date', 'n_cashflow_act', 'n_cashflow_inv_act', 'n_cashflow_fnc_act']])
    else:
        print("未查询到数据")


def test_all_symbols():
    """测试获取所有股票代码"""
    print("\n=== 测试8: 获取所有有财务数据的股票 ===")
    
    reader = FinancialDataReader(config)
    
    symbols = reader.get_all_symbols()
    print(f"共有 {len(symbols)} 只股票有财务数据")
    if len(symbols) > 0:
        print(f"示例股票代码: {symbols[:10]}")


def main():
    """主函数"""
    print("=" * 60)
    print("财务数据功能测试")
    print("=" * 60)
    
    try:
        # 运行所有测试
        test_basic_query()
        test_latest_financial()
        test_quarterly_data()
        test_time_series()
        test_cross_section()
        test_balance_sheet()
        test_cashflow()
        test_all_symbols()
        
        print("\n" + "=" * 60)
        print("所有测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

