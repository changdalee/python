# import akshare as ak
import io
import os
import sqlite3
import sys
import time
from datetime import datetime
from sqlite3 import OperationalError

import pandas as pd
import tushare as ts


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


def export_to_ths_txt(df, group_name="myselect_stocks"):
    """导出为同花顺TXT格式"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{group_name}_{timestamp}.txt"
    # current_path = os.getcwd()
    current_path = "D:\\"
    filepath = os.path.join(current_path, filename)
    print(f"正在导出文件到: {filepath}...")
    """
    for i in range(len(df)):
        code_change = df.at[i, "code"]

        if code_change >= "300000" and code_change < "600000":
            df.at[i, "code"] = "999999"
    """
    formatted_codes = df["code"]

    # 同花顺标准格式：每行一个6位股票代码
    with open(filepath, "w", encoding="gbk") as f:  # 重要：使用GBK编码
        f.write("代码    \n")
        for code in formatted_codes:
            f.write(code + "    \n")

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

    db_path = r"D:\develops\aktushare.db"

    token = "055680ead4592f1287876ef50197e46a76516c86268a33b8c0c565b0"
    ts.set_token(token)
    # print(ts.__version__)
    pro = ts.pro_api()

    print_hi("PyCharm")
    now_time = datetime.now().strftime("%H%M")

    today = datetime.now().strftime("%Y%m%d")

    conn = sqlite3.connect(
        db_path
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM stock_days where days <" + today
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_days = pd.DataFrame(rows, columns=["days"])

    daybefore1 = df_days["days"].iloc[-1]
    print("daybefore1=", daybefore1)

    daybefore10 = df_days["days"].iloc[-10]
    print("daybefore10=", daybefore10)

    df_daybf1 = pro.daily(trade_date=daybefore1).fillna(0)
    time.sleep(1)
    df_daybf10 = pro.daily(trade_date=daybefore10).fillna(0)

    df_bf1 = df_daybf1[["ts_code", "close"]]
    df_bf1.columns = ["ts_code", "close_bf1"]

    df_bf10 = df_daybf10[["ts_code", "close"]]
    df_bf10.columns = ["ts_code", "close_bf10"]

    df = pd.merge(df_bf1, df_bf10, on="ts_code", how="left")
    df["up_25%"] = (df["close_bf1"] - df["close_bf10"]) / df["close_bf10"] * 100
    df = df[df["up_25%"] > 25]
    df["up_25%"] = df["up_25%"].map("{:,.2f}".format)
    df["code"] = df["ts_code"].str[:6]
    df = df[df["code"] < "688000"]
    print(df)
    print("\n" + "_" * 99 + "\n")
    # exit(1)
    conn = sqlite3.connect(
        db_path
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
            "ts_code",
            "close_bf1",
            "close_bf10",
            "up_25%",
            "code",
            "name",
        ]
    ]

    df = df.rename(
        columns={
            "ts_code": "ts_code",
            "close_bf1": "close_bf1",
            "close_bf10": "close_bf10",
            "up_25%": "up_25%",
            "code": "code",
            "name": "name",
        }
    )
    print("\n" + "$" * 80 + "\n")
    print(df)
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_select_10days_up_25%",
        db_name=db_path,
        if_exists="replace",
    )

    export_to_ths_txt(df)
