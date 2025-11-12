import pybroker
from pybroker.ext.data import AKShare
import pandas as pd
import sqlite3
from sqlite3 import OperationalError

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #对pandas配置，列名与数据对其显示
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    #pd.set_option('display.max_rows', None)
    pybroker.enable_data_source_cache('AKShare')
    akshare = AKShare()
    # You can substitute 000001.SZ with 000001, and it will still work!
    # and you can set start_date as "20210301" format
    # You can also set adjust to 'qfq' or 'hfq' to adjust the data,
    # and set timeframe to '1d', '1w' to get daily, weekly data
    df = akshare.query(
        symbols=['000001.SZ', '600000.SH'],
        start_date='1/1/2025',
        end_date='7/18/2025',
        adjust="",
        timeframe="1d",
    )
    print(df)