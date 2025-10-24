"""
补齐财务指标新字段的脚本
用途：重新获取财务指标数据，补齐 q_roe 和 q_dt_roe 字段
运行方式: python 补齐财务指标新字段.py
"""

import sys
sys.path.append('.')

from panda_common.config import config
from panda_data_hub.data.tushare_financial_cleaner import FinancialDataCleanerTS
from datetime import datetime

def main():
    print("=" * 70)
    print("财务指标新字段补齐工具")
    print("=" * 70)
    print(f"新增字段: q_roe (净资产收益率-单季度), q_dt_roe (净资产单季度收益率-扣非)")
    print("=" * 70)
    
    # 配置选项
    print("\n请选择补齐方式：")
    print("1. 补齐所有历史数据（2017-至今）")
    print("2. 补齐最近2年数据（2023-至今）")
    print("3. 补齐最近4个季度")
    print("4. 自定义日期范围")
    print("5. 指定特定股票")
    
    choice = input("\n请输入选项 (1-5): ").strip()
    
    # 根据选择设置参数
    if choice == "1":
        start_date = "20170101"
        end_date = datetime.now().strftime("%Y%m%d")
        symbols = None
        print(f"\n将补齐 {start_date} 到 {end_date} 的所有股票数据")
        
    elif choice == "2":
        start_date = "20230101"
        end_date = datetime.now().strftime("%Y%m%d")
        symbols = None
        print(f"\n将补齐 {start_date} 到 {end_date} 的所有股票数据")
        
    elif choice == "3":
        # 最近4个季度
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        if current_month <= 3:
            quarters = [f"{current_year-1}1231", f"{current_year-1}0930", f"{current_year-1}0630", f"{current_year-1}0331"]
        elif current_month <= 6:
            quarters = [f"{current_year}0331", f"{current_year-1}1231", f"{current_year-1}0930", f"{current_year-1}0630"]
        elif current_month <= 9:
            quarters = [f"{current_year}0630", f"{current_year}0331", f"{current_year-1}1231", f"{current_year-1}0930"]
        else:
            quarters = [f"{current_year}0930", f"{current_year}0630", f"{current_year}0331", f"{current_year-1}1231"]
        
        start_date = quarters[-1]
        end_date = quarters[0]
        symbols = None
        print(f"\n将补齐最近4个季度的数据: {', '.join(quarters)}")
        
    elif choice == "4":
        start_date = input("请输入开始日期 (格式: YYYYMMDD): ").strip()
        end_date = input("请输入结束日期 (格式: YYYYMMDD): ").strip()
        symbols = None
        print(f"\n将补齐 {start_date} 到 {end_date} 的数据")
        
    elif choice == "5":
        symbols_input = input("请输入股票代码（多个用逗号分隔，如: 000001.SZ,600000.SH): ").strip()
        symbols = [s.strip() for s in symbols_input.split(',')]
        start_date = input("请输入开始日期 (格式: YYYYMMDD, 默认20170101): ").strip() or "20170101"
        end_date = input("请输入结束日期 (格式: YYYYMMDD, 默认今天): ").strip() or datetime.now().strftime("%Y%m%d")
        print(f"\n将补齐股票 {symbols} 从 {start_date} 到 {end_date} 的数据")
        
    else:
        print("无效的选项！")
        return
    
    # 确认
    confirm = input("\n确认开始补齐? (y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消")
        return
    
    print("\n" + "=" * 70)
    print("开始补齐数据...")
    print("=" * 70)
    
    # 创建清洗器实例
    cleaner = FinancialDataCleanerTS(config)
    
    # 定义进度回调
    def progress_callback(progress_info):
        percent = progress_info.get('progress_percent', 0)
        task = progress_info.get('current_task', '')
        processed = progress_info.get('processed_count', 0)
        total = progress_info.get('total_count', 0)
        print(f"进度: {percent:.1f}% | {task} | {processed}/{total}")
    
    cleaner.set_progress_callback(progress_callback)
    
    try:
        # 执行清洗（只清洗 indicator 类型）
        cleaner.financial_history_clean(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            data_types=['indicator']  # 只清洗财务指标
        )
        
        print("\n" + "=" * 70)
        print("补齐完成！")
        print("=" * 70)
        print("\n你可以通过以下方式验证：")
        print("1. 访问 http://localhost:8080/data-statistics 查看统计")
        print("2. 运行 python test_view_financial_data.py 查看数据")
        print("3. 在 MongoDB 中查询 financial_indicator 集合")
        
    except Exception as e:
        print(f"\n补齐失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

