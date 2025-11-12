import tushare as ts
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
import time
from datetime import date, timedelta, datetime
import sys
import io

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf8"
    )  # 强制标准输出UTF-8编码
    # 对pandas配置，列名与数据对其显示
    pd.set_option("display.unicode.ambiguous_as_wide", True)
    pd.set_option("display.unicode.east_asian_width", True)
    # 显示所有列
    pd.set_option("display.max_columns", None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)

    token = "055680ead4592f1287876ef50197e46a76516c86268a33b8c0c565b0"
    ts.set_token(token)
    # print(ts.__version__)
    today = datetime.now().strftime("%Y%m%d")

    pro = ts.pro_api()

    df = pro.daily(trade_date=today)
    print(df)
    print(df.empty)
    print(len(df))
    print("Python默认编码:", sys.getdefaultencoding())  # 输出应为'utf-8'

    print("Python标准输出编码:", sys.stdout.encoding)  # 应为'utf-8'
