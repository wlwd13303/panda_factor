import pandas as pd
import numpy as np

def get_exchange_suffix(code):
    code = code.split('.')[0]
    if code.startswith(("600", "601", "603", "688", "689")):
        return f"{code}.SH"  # 上海证券交易所
    elif code.startswith(("000", "001", "300", "320")):
        return f"{code}.SZ"  # 深圳证券交易所
    elif code.startswith(("43", "83")):
        return f"{code}.BJ"  # 北京证券交易所
    else:
        return "UNKNOWN"

def get_exchange_suffix_tqsdk(code):
    code = code.split('.')[1]
    if code.startswith(("600", "601", "603", "688", "689")):
        return f"{code}.SH"  # 上海证券交易所
    elif code.startswith(("000", "001", "300", "320")):
        return f"{code}.SZ"  # 深圳证券交易所
    elif code.startswith(("43", "83")):
        return f"{code}.BJ"  # 北京证券交易所
    else:
        return "UNKNOWN"