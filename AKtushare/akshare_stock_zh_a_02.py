import datetime

import akshare as ak
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
from datetime import date, timedelta, datetime


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press F9 to toggle the breakpoint.


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
        '''
        c = conn.cursor()
        print("数据库打开成功")
        c.execute("DELETE * from {table_name}")
        conn.commit()
        '''
        # 将DataFrame写入SQLite表
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=index
        )

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
if __name__ == '__main__':
    # 对pandas配置，列名与数据对其显示
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)

    print_hi('PyCharm')

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM select_stock_zh_a_01")
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    dt = pd.DataFrame(rows,
                      columns=['code', 'name', 'open', 'current', 'volume_ratio', 'turnover_ratio', 'total_capital', 'trade_capital',
                               'trade_volume', 'date'])

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_rank_xstp5_cleaned")
    row5 = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    d5 = pd.DataFrame(row5, columns=['code', 'name', 'open'])

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_rank_xstp10_cleaned")
    # 获取所有结果:ml-citation{ref="6" data="citationList"}
    row10 = cursor.fetchall()
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    d10 = pd.DataFrame(row10, columns=['code', 'name', 'open'])

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_rank_xstp20_cleaned")
    # 获取所有结果:ml-citation{ref="6" data="citationList"}
    row20 = cursor.fetchall()
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    d20 = pd.DataFrame(row20, columns=['code', 'name', 'open'])

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_rank_xstp30_cleaned")
    # 获取所有结果:ml-citation{ref="6" data="citationList"}
    row30 = cursor.fetchall()
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    d30 = pd.DataFrame(row30, columns=['code', 'name', 'open'])

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_days")
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(rows, columns=['days'])

    today = datetime.now().strftime("%Y%m%d")
    next = df[(df['days'] > today)]
    # print(next)
    nextday = next.iloc[0]['days']
    # print(nextday)

    before = df[(df['days'] < today)]
    # print(before)
    before_day01 = before.iloc[len(before)-1]['days']

    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect('akshare.db')
    cursor = conn.cursor()
    # +before_day01)  # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM select_stock_zh_a_01")
    # 获取所有结果:ml-citation{ref="6" data="citationList"}
    rows_daybf1 = cursor.fetchall()
    # print(rows_daybf1)
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_daybf1 = pd.DataFrame(rows_daybf1, columns=[
                             'code', 'name', 'open', 'current', 'volume_ratio', 'turnover_ratio', 'total_capital', 'trade_capital', 'trade_volume', 'date'])
    print("\n" + "@" * 80 + "\n")
    print(df_daybf1)

    dt1 = dt[dt['code'].isin(d5['code'])]
    dt2 = dt1[dt1['code'].isin(d10['code'])]
    dt3 = dt2[dt2['code'].isin(d20['code'])]
    df1 = dt3[dt3['code'].isin(d30['code'])]

    df = df1[df1['code'].isin(df_daybf1['code'])]

    print("\n" + "_" * 80 + "\n")
    print(df)
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='select_stock_zh_a_02_' + today,
        db_name='akshare.db',
        if_exists='replace'
    )

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='select_stock_zh_a_02',
        db_name='akshare.db',
        if_exists='replace'
    )
