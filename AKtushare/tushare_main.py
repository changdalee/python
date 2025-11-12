import tushare as ts
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
import time
from datetime import date, timedelta, datetime


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f"Hi, {name}")  # Press F9 to toggle the breakpoint.


def df_to_sqlite(df, table_name, db_name, if_exists, index=False):
    """
    将pandas DataFrame存储到SQLite3数据库

    参数:
        df: 要存储的DataFrame
        table_name: 要创建的表名
        db_name: SQLite数据库文件名，默认为'data.db'
        if_exists: 表存在时的处理方式，可选'replace'、'append'、'fail'
        index: 是否将DataFrame的索引作为一列存储
    """
    try:
        # 连接到SQLite数据库（如果不存在则创建）
        conn = sqlite3.connect(db_name)
        """
        c = conn.cursor()
        print("数据库打开成功")
        c.execute("DELETE from {table_name};")
        conn.commit()
        """
        # 将DataFrame写入SQLite表
        df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=index)

        # 提交事务并关闭连接
        conn.commit()
        conn.close()

        print(f"成功将DataFrame存储到SQLite表 '{table_name}'，共 {len(df)} 行数据")
        return True

    except OperationalError as e:
        print(f"数据库操作错误: {str(e)}")
        return False
    except Exception as e:
        print(f"发生错误: {str(e)}")
        return False


def get_daily(self, ts_code="", trade_date="", start_date="", end_date=""):
    for _ in range(3):
        # try:
        if trade_date:
            df = self.pro.daily(ts_code=ts_code, trade_date=trade_date)
        else:
            df = self.pro.daily(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            # except TypeError:
            time.sleep(1)
    # else:
    #        return df


if __name__ == "__main__":
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

    print_hi("PyCharm")

    today = datetime.now().strftime("%Y%m%d")
    daybf1 = datetime.now() - timedelta(days=1)
    daybefore1 = daybf1.strftime("%Y%m%d")
    daybf2 = datetime.now() - timedelta(days=2)
    daybefore2 = daybf2.strftime("%Y%m%d")
    daybf3 = datetime.now() - timedelta(days=3)
    daybefore3 = daybf3.strftime("%Y%m%d")
    daybf4 = datetime.now() - timedelta(days=4)
    daybefore4 = daybf4.strftime("%Y%m%d")
    daybf5 = datetime.now() - timedelta(days=5)
    daybefore5 = daybf5.strftime("%Y%m%d")

    day_saved = daybefore5
    pro = ts.pro_api()
    df = pro.daily(trade_date=day_saved)
    print(df)
    print("\n" + "_" * 99 + "\n")
    """
    df = pro.trade_cal(exchange='SSE', is_open='1',
                       start_date='20250101',
                       end_date='20250718',
                       fields='cal_date')
    print(df)
    """

    print("\n" + "_" * 99 + "\n")
    """
    for date in df['cal_date'].values:
        df = get_daily(date)
    """

    """
    df = pd.DataFrame(stock_rank_cxfl_ths_df)
    # 方法1: 直接通过列名列表选择（最常用）
    selected_cols = ['股票代码', '股票简称', '最新价']
    df1 = df[selected_cols]

    df_cleaned = df1[~df1['股票简称'].str.contains('ST', na=False)]
    df = df_cleaned[~df_cleaned['股票简称'].str.contains('退', na=False)]
    df = df[~df['股票简称'].str.contains('PT', na=False)]

    print("方法1 - 选择指定列名:")
    print(df)
    print("\n" + "_" * 80 + "\n")
    """
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_stock_daily_" + day_saved,
        db_name="akshare.db",
        if_exists="replace",
    )
