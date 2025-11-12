import akshare as ak
import pandas as pd
import sqlite3
import time
from sqlite3 import OperationalError
from datetime import datetime


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
        c.execute("DELETE * from {table_name}")
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


def calc_volume_ratio(stock_code):
    # 获取历史数据（含成交量）
    df = ak.stock_zh_a_hist(
        symbol=stock_code,
        period="daily",
        start_date="20250707",
        end_date="20250715",
        adjust="",
    )

    # 计算5日平均成交量（不含当日）
    df["5d_avg_vol"] = df["trade_volume"].rolling(5).mean().shift(1)

    # 计算量比（收盘后简化版）
    df["volume_ratio"] = df["trade_volume"] / df["5d_avg_vol"]
    data = df[["代码", "名称", "date", "trade_volume", "5d_avg_vol", "volume_ratio"]]
    df = df.rename(
        columns={
            "代码": "code",
            "名称": "name",
            "date": "date",
            "volume_ratio": "current",
            "trade_volume": "trade_volume",
            "5d_avg_vol": "5d_avg_vol",
        }
    )
    return df[["code", "name", "date", "volume_ratio", "trade_volume", "5d_avg_vol"]]


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # 对pandas配置，列名与数据对其显示
    pd.set_option("display.unicode.ambiguous_as_wide", True)
    pd.set_option("display.unicode.east_asian_width", True)
    # 显示所有列
    pd.set_option("display.max_columns", None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)

    print_hi("PyCharm")

    conn = sqlite3.connect(
        "akshare.db"
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM selected_zh_a_up2to5"
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(
        columns=["date", "volume_ratio", "trade_volume", "5d_avg_vol"])
    for row in rows:
        stock_code = row[0]
        df = calc_volume_ratio(stock_code)
        print("\n" + "_" * 80 + "\n")
        print(df)
        # 存储到SQLite数据库
        df_to_sqlite(
            df=df, table_name="volsma_zh_a", db_name="akshare.db", if_exists="append"
        )
        print_hi("PyCharm")
        time.sleep(60)
