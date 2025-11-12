import sqlite3
from sqlite3 import OperationalError
from datetime import date, datatime, timedelta
from chinese_calendar import is_workday, is_holiday
import pandas as pd


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


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # 对pandas配置，列名与数据对其显示
    pd.set_option("display.unicode.ambiguous_as_wide", True)
    pd.set_option("display.unicode.east_asian_width", True)
    # 显示所有列
    pd.set_option("display.max_columns", None)
    # 显示所有行
    pd.set_option("display.max_rows", None)

    print_hi("PyCharm")

    # 创建日期
    start_date = datetime(2025, 1, 1)
    num_days = 365
    date_list = []

    selected_cols = ["days"]
    df = pd.DataFrame(columns=selected_cols)

    current_date = start_date
    count = 0

    while count < num_days:
        if is_workday(current_date) and current_date.weekday() < 5:
            date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
        count += 1

    df["days"] = date_list

    # print(df)

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df, table_name="stock_days", db_name="akshare.db", if_exists="replace"
    )

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

    before = df[(df["days"] < today)]
    # print(before)
    before_day01 = before.iloc[len(before) - 1]["days"]
    # print(before_day01)
    before_day02 = before.iloc[len(before) - 2]["days"]
    # print(before_day02)
    before_day03 = before.iloc[len(before) - 3]["days"]
    # print(before_day03)
    before_day04 = before.iloc[len(before) - 4]["days"]
    # print(before_day04)
    before_day05 = before.iloc[len(before) - 5]["days"]
    # print(before_day05)
