"""
计算单只股票的前复权行情
用法: python scripts/calc_adj_price.py 600519 [start_date] [end_date]

前复权价格 = 不复权价格 × 当日复权因子 / 最新复权因子
"""

import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import panda_data


def _get_tushare_suffix(code: str) -> str:
    """根据股票代码前缀返回 Tushare ts_code 后缀"""
    code = code.split(".")[0]
    if code.startswith(("600", "601", "603", "688", "689", "605", "900")):
        return f"{code}.SH"
    elif code.startswith(("000", "001", "300", "200", "002", "301", "201", "003", "302")):
        return f"{code}.SZ"
    elif code.startswith(("43", "83", "87", "920")):
        return f"{code}.BJ"
    else:
        raise ValueError(f"无法识别股票代码: {code}")


def calc_adj_prices(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取不复权行情和复权因子，计算前复权 OHLC 价格

    参数:
        symbol: 股票代码，如 "600519"
        start_date: 起始日期 YYYYMMDD，如 "20200101"
        end_date: 结束日期 YYYYMMDD，如 "20250519"

    返回:
        DataFrame 包含: date, symbol,
                        open/high/low/close/volume (不复权原始值),
                        adj_factor (复权因子),
                        fq_open/fq_high/fq_low/fq_close (前复权)
    """
    ts_code = _get_tushare_suffix(symbol)

    print(f"正在获取 {symbol} 的行情数据 ({start_date} ~ {end_date})...")
    df_market = panda_data.get_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbol,
        indicator="000985",
        st=True,
        fields=["open", "high", "low", "close", "volume", "pre_close"],
    )
    if df_market is None or df_market.empty:
        raise RuntimeError(f"未找到 {symbol} 的行情数据")

    df_market = df_market.sort_values("date").reset_index(drop=True)
    print(f"  获取到 {len(df_market)} 条行情记录")

    print(f"正在获取 {ts_code} 的复权因子...")
    df_adj = panda_data.get_adj_factor(
        symbols=ts_code,
        start_date=start_date,
        end_date=end_date,
    )

    if df_adj is None or df_adj.empty:
        raise RuntimeError(f"未找到 {ts_code} 的复权因子数据")

    df_adj = df_adj.sort_values("date").reset_index(drop=True)
    print(f"  获取到 {len(df_adj)} 条复权因子记录")

    # 将 market 的 symbol 转为 ts_code 格式，与 adj_factor 对齐合并
    df_market["symbol"] = df_market["symbol"].apply(_get_tushare_suffix)
    df = df_market.merge(df_adj[["date", "symbol", "adj_factor"]], on=["date", "symbol"], how="left")

    # 前向填充复权因子（填补非交易日缺失）
    df["adj_factor"] = df["adj_factor"].ffill()

    missing_adj = df["adj_factor"].isna().sum()
    if missing_adj > 0:
        print(f"  警告: 有 {missing_adj} 条记录缺少复权因子，将使用 1.0 填充")
        df["adj_factor"] = df["adj_factor"].fillna(1.0)

    latest_adj = df["adj_factor"].iloc[-1]
    print(f"  最新复权因子: {latest_adj}")

    if latest_adj <= 0:
        raise RuntimeError(f"最新复权因子异常: {latest_adj}")

    # 计算前复权价格: 不复权价格 × 当日复权因子 / 最新复权因子
    for col in ["open", "high", "low", "close"]:
        df[f"fq_{col}"] = df[col] * df["adj_factor"] / latest_adj

    cols = [
        "date", "symbol",
        "open", "high", "low", "close", "volume",
        "adj_factor",
        "fq_open", "fq_high", "fq_low", "fq_close",
    ]
    df = df[[c for c in cols if c in df.columns]]

    return df


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    symbol = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) > 2 else "20230101"
    end_date = sys.argv[3] if len(sys.argv) > 3 else "20250519"

    print("初始化 panda_data...")
    panda_data.init()

    df = calc_adj_prices(symbol, start_date, end_date)

    plain_code = symbol.split(".")[0]
    output_path = os.path.join(os.path.dirname(__file__), f"adj_price_{plain_code}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存到: {output_path}")

    print(f"\n{'='*120}")
    print(f"{symbol} 复权行情预览 (前复权 = 不复权 × adj_factor / {df['adj_factor'].iloc[-1]:.4f})")
    print(f"{'='*120}")
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_rows", 30)
    print(df.head(10))
    print("...")
    print(df.tail(10))
    print(f"\n共 {len(df)} 条记录，日期范围: {df['date'].min()} ~ {df['date'].max()}")


if __name__ == "__main__":
    main()
