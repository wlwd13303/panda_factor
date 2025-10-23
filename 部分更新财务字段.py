"""
部分更新财务字段工具
用途：只更新指定的字段，节省 Tushare API 积分
运行方式: python 部分更新财务字段.py
"""

import sys
sys.path.append('.')

from panda_common.config import config
from panda_data_hub.data.tushare_financial_cleaner_enhanced import FinancialDataCleanerTSEnhanced
from panda_common.handlers.database_handler import DatabaseHandler
from datetime import datetime

def get_all_symbols():
    """获取所有股票代码"""
    db_handler = DatabaseHandler(config)
    collection = db_handler.get_mongo_collection(config["MONGO_DB"], "financial_indicator")
    symbols = collection.distinct("symbol")
    return sorted(symbols)

def get_recent_quarters(count=4):
    """获取最近N个季度"""
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
    
    quarters = []
    year = current_year
    quarter = current_quarter
    
    for i in range(count):
        if quarter == 1:
            end_date = f"{year}0331"
        elif quarter == 2:
            end_date = f"{year}0630"
        elif quarter == 3:
            end_date = f"{year}0930"
        else:
            end_date = f"{year}1231"
        
        quarters.append(end_date)
        
        # 往前推一个季度
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    
    return quarters

def main():
    print("=" * 70)
    print("部分更新财务字段工具 - 节省API调用")
    print("=" * 70)
    print("功能：只更新指定的字段，不获取全部字段，节省 Tushare API 积分")
    print("=" * 70)
    
    # 创建增强版清洗器
    cleaner = FinancialDataCleanerTSEnhanced(config)
    
    # 步骤1：选择要更新的字段
    print("\n【步骤1】选择要更新的字段")
    print("-" * 70)
    print("常见字段：")
    print("  1. q_roe, q_dt_roe          - 单季度ROE（你新增的字段）")
    print("  2. roe, roe_waa, roe_dt     - ROE相关字段")
    print("  3. roa, roa2_yearly         - ROA相关字段")
    print("  4. eps, dt_eps              - EPS相关字段")
    print("  5. 自定义字段")
    
    choice = input("\n请选择 (1-5): ").strip()
    
    if choice == "1":
        fields_to_update = ['q_roe', 'q_dt_roe']
    elif choice == "2":
        fields_to_update = ['roe', 'roe_waa', 'roe_dt']
    elif choice == "3":
        fields_to_update = ['roa', 'roa2_yearly']
    elif choice == "4":
        fields_to_update = ['eps', 'dt_eps']
    elif choice == "5":
        custom_fields = input("请输入字段名（多个用逗号分隔）: ").strip()
        fields_to_update = [f.strip() for f in custom_fields.split(',')]
    else:
        print("无效选项")
        return
    
    print(f"\n将更新字段: {', '.join(fields_to_update)}")
    
    # 步骤2：选择股票范围
    print("\n【步骤2】选择股票范围")
    print("-" * 70)
    print("  1. 所有股票")
    print("  2. 指定股票代码")
    print("  3. 随机测试（5只股票）")
    
    choice = input("\n请选择 (1-3): ").strip()
    
    if choice == "1":
        print("\n正在获取所有股票代码...")
        symbols = get_all_symbols()
        print(f"共 {len(symbols)} 只股票")
    elif choice == "2":
        symbols_input = input("请输入股票代码（多个用逗号分隔）: ").strip()
        symbols = [s.strip() for s in symbols_input.split(',')]
    elif choice == "3":
        symbols = ['000001.SZ', '000002.SZ', '000333.SZ', '600000.SH', '600519.SH']
        print(f"测试股票: {', '.join(symbols)}")
    else:
        print("无效选项")
        return
    
    # 步骤3：选择报告期范围
    print("\n【步骤3】选择报告期范围")
    print("-" * 70)
    print("  1. 最近1个季度")
    print("  2. 最近2个季度")
    print("  3. 最近4个季度")
    print("  4. 所有季度（2017-至今）")
    print("  5. 自定义报告期")
    
    choice = input("\n请选择 (1-5): ").strip()
    
    if choice == "1":
        periods = get_recent_quarters(1)
    elif choice == "2":
        periods = get_recent_quarters(2)
    elif choice == "3":
        periods = get_recent_quarters(4)
    elif choice == "4":
        # 生成2017年至今所有季度
        periods = []
        current_year = datetime.now().year
        for year in range(2017, current_year + 1):
            for quarter_end in ['0331', '0630', '0930', '1231']:
                period = f"{year}{quarter_end}"
                periods.append(period)
    elif choice == "5":
        periods_input = input("请输入报告期（YYYYMMDD格式，多个用逗号分隔）: ").strip()
        periods = [p.strip() for p in periods_input.split(',')]
    else:
        print("无效选项")
        return
    
    print(f"\n将更新报告期: {', '.join(periods[:5])}{'...' if len(periods) > 5 else ''} (共{len(periods)}个)")
    
    # 计算预估API调用次数
    total_calls = len(symbols) * len(periods)
    print(f"\n预估API调用次数: {total_calls}")
    if total_calls > 100:
        print(f"⚠️  警告：将消耗约 {total_calls} 次API调用")
    
    # 确认
    confirm = input(f"\n确认开始部分更新? (y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消")
        return
    
    # 开始更新
    print("\n" + "=" * 70)
    print("开始部分更新...")
    print("=" * 70)
    
    try:
        result = cleaner.clean_financial_indicator_partial(
            symbols=symbols,
            periods=periods,
            fields_to_update=fields_to_update,
            mode='partial'  # 部分更新模式
        )
        
        print("\n" + "=" * 70)
        print("✅ 部分更新完成！")
        print("=" * 70)
        print(f"处理任务数: {result['processed']} / {result['total']}")
        print(f"更新模式: {result['mode']}")
        print(f"更新字段: {result['fields']}")
        
        # 验证示例
        if symbols:
            print("\n【验证】查看第一只股票的数据:")
            print("-" * 70)
            
            from panda_data.panda_data.financial.financial_data_reader import FinancialDataReader
            reader = FinancialDataReader(config)
            
            verify_fields = ['symbol', 'end_date', 'ann_date'] + fields_to_update
            df = reader.get_financial_data(
                symbols=[symbols[0]],
                fields=verify_fields,
                data_type='indicator'
            )
            
            if df is not None and not df.empty:
                print(df.head())
                
                # 检查字段是否有值
                for field in fields_to_update:
                    if field in df.columns:
                        has_data = df[field].notna().any()
                        print(f"\n{field} 字段是否有数据: {'✅ 是' if has_data else '❌ 否'}")
            else:
                print("未查询到数据")
        
        print("\n" + "=" * 70)
        print("提示:")
        print("1. 访问 http://localhost:8080/data-statistics 查看统计")
        print("2. 运行 python test_view_financial_data.py 查看完整数据")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ 部分更新失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

