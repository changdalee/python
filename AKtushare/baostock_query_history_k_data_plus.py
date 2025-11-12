import io
import sqlite3
import sys
from datetime import datetime
from sqlite3 import OperationalError

import baostock as bs
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


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf8")  # 强制标准输出UTF-8编码
    # 对pandas配置，列名与数据对其显示
    pd.set_option("display.unicode.ambiguous_as_wide", True)
    pd.set_option("display.unicode.east_asian_width", True)
    # 显示所有列
    pd.set_option("display.max_columns", None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)
    today = datetime.now().strftime("%Y%m%d")
    print(today)
    db_path = r'D:\develops\python\aktushare.db'
    conn = sqlite3.connect(db_path)  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM stock_days where days <=" + today)  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_days = pd.DataFrame(rows, columns=["days"])
    daybefore1 = df_days["days"].iloc[-1]

    conn = sqlite3.connect(db_path)  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM stock_basic_plus")  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(rows, columns=["code", "name", "ak_code", "tu_code", "bao_code"])
    df["peTTM"] = 0
    select_day = daybefore1[0:4] + "-" + daybefore1[4:6] + "-" + daybefore1[6:8]
    df["date"] = select_day
    print(select_day)
    #### 获取历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节
    data_list = []
    print("\n使用 itertuples() 遍历:")
    # for row in df.itertuples():
    # print(f"code: {row.code}, name: {row.name}, ak_code: {row.ak_code}, tu_code: {row.tu_code}, bao_code: {row.bao_code}")
    rs = bs.query_history_k_data_plus(code=df["bao_code"], fields="peTTM", start_date=select_day, end_date=select_day,
                                      frequency="d", adjustflag="3")  # frequency="d"取日k线，adjustflag="3"默认不复权
    # print('query_history_k_data_plus respond error_code:' + rs.error_code)
    # print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    # print(rs)
    print(rs.get_data())
    rows = rs.get_row_data()
    if len(rows) > 0:
        df.loc[df["code"] == row.code, "peTTM"] = rows[0]
    #### 打印结果集 ####
    """
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
        print(rs.get_row_data())
    """
# print(data_list)
df_output = pd.DataFrame(data_list, columns=rs.fields)
#### 结果集输出到csv文件 ####
df_to_sqlite(df=df_output, table_name="stock_basic_peTTM", db_name="aktushare.db", if_exists="replace", )

#### 登出系统 ####
bs.logout()
