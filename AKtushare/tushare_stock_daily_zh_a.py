import tushare as ts
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
import time
from datetime import date, timedelta, datetime
import sys
import io


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
    # today = '20250721'

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

    next = df[(df["days"] > today)]
    # print(next)
    next_day01 = next.iloc[0]["days"]
    # print(next)
    prier = df[(df["days"] < today)]
    # print(prier)
    prier_day01 = prier.iloc[len(prier) - 1]["days"]
    # print(prier_day01)
    prier_day02 = prier.iloc[len(prier) - 2]["days"]
    # print(prier_day02)

    pro = ts.pro_api()
    df00 = pro.daily(trade_date=today)
    print(df00)
    print("00000000000000000---------------------------------------------------")
    if df00.empty:
        df01 = pro.daily(trade_date=prier_day01)
        df02 = pro.daily(trade_date=prier_day02)
        df = pd.merge(df01, df02, on="ts_code")
        df["date"] = prier_day01
        day_saved = prier_day01
    else:
        df01 = pro.daily(trade_date=prier_day01)
        df = pd.merge(df00, df01, on="ts_code")
        df["date"] = today
        day_saved = today
    df["code"] = df01["ts_code"].apply(lambda x: x[:6])

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
    df = df[df["code"].apply(lambda x: not str(x) > "687999")]
    print(df)

    df = df.drop(df[df["vol_y"] <= 0].index)
    df["vol_ratio"] = df["vol_x"] / df["vol_y"]
    df["vol_ratio"] = df["vol_ratio"].round(2)
    df = df.drop(df[df["vol_ratio"] < 1.5].index)
    # df[['date', 'code']] = df[['code', 'date']].values
    df = df[
        [
            "code",
            "name",
            "vol_ratio",
            "date",
            "ts_code",
            "trade_date_x",
            "open_x",
            "high_x",
            "close_x",
        ]
    ]
    print(df)
    print("\n" + "_" * 80 + "\n")

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_stock_daily_" + day_saved,
        db_name="akshare.db",
        if_exists="replace",
    )
