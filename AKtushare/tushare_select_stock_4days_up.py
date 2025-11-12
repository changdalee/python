# import akshare as ak
import tushare as ts
import pandas as pd
import time
import sqlite3
from sqlite3 import OperationalError
from datetime import datetime
import io
import sys
import os


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
        # 将DataFrame写入SQLite表+
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


def export_to_ths_txt(df, group_name='myselect_stocks'):
    """导出为同花顺TXT格式"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{group_name}_{timestamp}.txt"
    # current_path = os.getcwd()
    current_path = "D:\\"
    filepath = os.path.join(current_path, filename)
    print(f"正在导出文件到: {filepath}...")
    formatted_codes = df["code"]

    # 同花顺标准格式：每行一个6位股票代码
    with open(filepath, 'w', encoding='gbk') as f:  # 重要：使用GBK编码
        f.write('代码    \n')
        for code in formatted_codes:
            f.write(code + '    \n')

    print(f"✅ 成功导出 {len(formatted_codes)} 只股票到: {filepath}")
    return filepath


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

    print_hi("PyCharm")

    today = datetime.now().strftime("%Y%m%d")
    conn = sqlite3.connect(
        "aktushare.db"
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM stock_days where days <=" + today
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_days = pd.DataFrame(rows, columns=["days"])

    daybefore1 = df_days["days"].iloc[-1]
    if daybefore1 == today:
        daybefore1 = df_days["days"].iloc[-2]
        daybefore2 = df_days["days"].iloc[-3]
        daybefore3 = df_days["days"].iloc[-4]
        daybefore4 = df_days["days"].iloc[-5]

    else:
        daybefore2 = df_days["days"].iloc[-2]
        daybefore3 = df_days["days"].iloc[-3]
        daybefore4 = df_days["days"].iloc[-4]
    pro = ts.pro_api()
    df_daybf1 = pro.daily(trade_date=daybefore1).fillna(0)
    print("daybefore1")
    print(daybefore1)
    print(df_daybf1)
    time.sleep(3)
    df_daybf2 = pro.daily(trade_date=daybefore2).fillna(0)
    print("daybefore2")
    print(daybefore2)
    print(df_daybf2)
    time.sleep(3)
    df_daybf3 = pro.daily(trade_date=daybefore3).fillna(0)
    print("daybefore3")
    print(daybefore3)
    print(df_daybf3)
    time.sleep(3)
    df_daybf4 = pro.daily(trade_date=daybefore4).fillna(0)
    print("daybefore4")
    print(daybefore4)
    print(df_daybf4)
    print("\n" + "A" * 99 + "\n")
    df_bf1 = df_daybf1[["ts_code", "vol", "close"]]
    df_bf1.columns = ["ts_code", "vol_bf1", "close_bf1"]
    print(df_bf1)
    df_bf2 = df_daybf2[["ts_code", "vol", "close"]]
    df_bf2.columns = ["ts_code", "vol_bf2", "close_bf2"]
    print(df_bf2)
    df_bf3 = df_daybf3[["ts_code", "vol", "close"]]
    df_bf3.columns = ["ts_code", "vol_bf3", "close_bf3"]
    print(df_bf3)
    df_bf4 = df_daybf4[["ts_code", "vol", "close"]]
    df_bf4.columns = ["ts_code", "vol_bf4", "close_bf4"]
    print(df_bf4)
    print("\n" + "B" * 99 + "\n")
    df = pd.merge(df_bf1, df_bf2, on="ts_code", how="left")
    df = pd.merge(df, df_bf3, on="ts_code", how="left")
    df = pd.merge(df, df_bf4, on="ts_code", how="left")
    print(df)
    df.fillna(0, inplace=True)
    print(df)

    df = df[df["ts_code"].apply(lambda x: not str(x) > "687999.AA")]
    df["code"] = df["ts_code"].apply(lambda x: x[:6])
    print(df)
    df = df.drop(df[df["vol_bf1"] < 1].index)
    df = df.drop(df[df["vol_bf2"] < 1].index)
    df = df.drop(df[df["vol_bf3"] < 1].index)
    df = df.drop(df[df["vol_bf4"] < 1].index)

    df = df.drop(df[df["close_bf1"] < 1].index)
    df = df.drop(df[df["close_bf2"] < 1].index)
    df = df.drop(df[df["close_bf3"] < 1].index)
    df = df.drop(df[df["close_bf4"] < 1].index)

    # print(df)

    df = df.drop(df[df["vol_bf1"] < df["vol_bf2"]].index)
    df = df.drop(df[df["vol_bf2"] < df["vol_bf3"]].index)
    df = df.drop(df[df["vol_bf3"] < df["vol_bf4"]].index)

    df = df.drop(df[df["close_bf1"] < df["close_bf2"]].index)
    df = df.drop(df[df["close_bf2"] < df["close_bf3"]].index)
    df = df.drop(df[df["close_bf3"] < df["close_bf4"]].index)

    # print(df)

    print("\n" + "_" * 99 + "\n")

    conn = sqlite3.connect(
        "akshare.db"
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM stock_basic"
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_name = pd.DataFrame(rows, columns=["code", "name"])
    # print(df_name)
    df = pd.merge(df, df_name, on="code", how="left")
    df = df[
        df["name"].apply(
            lambda x: "ST" not in str(x)
            and "*ST" not in str(x)
            and "PT" not in str(x)
            and "退" not in str(x)
        )
    ]
    # print(df)

    df = df[
        [
            "code",
            "name",
            "vol_bf1",
            "vol_bf2",
            "vol_bf3",
            "vol_bf4",
            "close_bf1",
            "close_bf2",
            "close_bf3",
            "close_bf4",
        ]
    ]

    df = df.rename(
        columns={
            "code": "code",
            "name": "name",
            "vol_bf1": "vol_bf1",
            "vol_bf2": "vol_bf2",
            "vol_bf3": "vol_bf3",
            "vol_bf4": "vol_bf4",
            "close_bf1": "close_bf1",
            "close_bf2": "close_bf2",
            "close_bf3": "close_bf3",
            "close_bf4": "close_bf4",
        }
    )
    df['PE_ratio'] =
    df['date'] = today
    print("\n" + "$" * 80 + "\n")
    print(df)
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_select_4days_up",
        db_name="aktushare.db",
        if_exists="replace",
    )
    export_to_ths_txt(df)
