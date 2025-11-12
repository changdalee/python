# -*- coding: utf-8 -*-
import akshare as ak
import tushare as ts
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import OperationalError
from datetime import date, time, timedelta, datetime
import io
import sys


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

    conn = sqlite3.connect(
        "akshare.db"
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM stock_days"
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(rows, columns=["days"])

    today = datetime.now().strftime("%Y%m%d")
    next = df[(df["days"] > today)]
    # print(next)
    nextday = next.iloc[0]["days"]
    # print(nextday)

    prierday = df[(df["days"] < today)]
    # print(before)
    before_day01 = prierday.iloc[len(prierday) - 1]["days"]

    # 查询所有股票的实时行情数据
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    df_current = stock_zh_a_spot_em_df.fillna(0)
    # print(df_current)

    df_current = df_current[df_current["代码"].apply(lambda x: not str(x) > "687999")]
    selected_cols = [
        "代码",
        "名称",
        "今开",
        "最新价",
        "量比",
        "换手率",
        "总市值",
        "流通市值",
        "成交量",
    ]
    df_current = df_current[selected_cols]
    df_current = df_current.rename(
        columns={
            "代码": "code",
            "名称": "name",
            "今开": "open",
            "最新价": "current",
            "量比": "vol_ratio",
            "换手率": "tur_ratio",
            "总市值": "total_capital",
            "流通市值": "trade_capital",
            "成交量": "trade_volume",
        }
    )
    df_current = df_current[
        df_current["name"].apply(
            lambda x: "ST" not in str(x)
            and "*ST" not in str(x)
            and "PT" not in str(x)
            and "退" not in str(x)
        )
    ]

    df_current = df_current[df_current["code"].apply(lambda x: not str(x) > "687999")]
    df_current = df_current.drop(df_current[df_current["current"] < 2].index)

    pro = ts.pro_api()
    df_yesterday = pro.daily(trade_date=before_day01).fillna(0)
    df_yesterday["code"] = df_yesterday["ts_code"].apply(lambda x: x[:6])
    # print(df_yesterday)
    df_yesterday = df_yesterday.drop(df_yesterday[df_yesterday["vol"] < 1].index)
    df = pd.merge(df_current, df_yesterday, on="code", how="left")

    print(df)
    df["vol_ratio"] = df["trade_volume"] / df["vol"]
    df["vol_ratio"] = df["vol_ratio"].round(2)
    df = df.drop(df[df["vol_ratio"] < 1.1].index)
    df = df.drop(df[df["vol_ratio"] > 1.5].index)
    df = df[
        [
            "code",
            "name",
            "trade_date",
            "current",
            "vol_ratio",
            "tur_ratio",
            "trade_capital",
            "trade_volume",
            "vol",
        ]
    ]
    df = df.rename(
        columns={
            "code": "code",
            "name": "name",
            "trade_date": "trade_date",
            "current": "current",
            "vol_ratio": "vol_ratio",
            "tur_ratio": "tur_ratio",
            "trade_capital": "trade_capital",
            "trade_volume": "trade_volume",
            "vol": "vol",
        }
    )
    # print("\n" + "%" * 80 + "\n")
    print(df)
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_select_stock_zh_current_01",
        db_name="akshare.db",
        if_exists="replace",
    )
    print("数据存储完成")
