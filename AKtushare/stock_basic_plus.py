import io
import sqlite3
import sys
from sqlite3 import OperationalError
import numpy as np
import pandas as pd
from datetime import datetime


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
if __name__ == '__main__':
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf8")  # 强制标准输出UTF-8编码
    # 对pandas配置，列名与数据对其显示
    pd.set_option("display.unicode.ambiguous_as_wide", True)
    pd.set_option("display.unicode.east_asian_width", True)
    # 显示所有列
    pd.set_option("display.max_columns", None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)

    print_hi('PyCharm')
    db_path = r'D:\develops\python\aktushare.db'
    # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # 执行查询:ml-citation{ref="10" data="citationList"}
    cursor.execute("SELECT * FROM stock_basic")
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(rows, columns=["code", "name"])
    df['ak_code'] = df['code']
    df['tu_code'] = np.where((df['code'] >= '600000'),
                             (df['code'] + '.SH'), (df['code'] + '.SZ'))
    df['bao_code'] = np.where(df['code'] >= '600000',
                              'sh.' + df['code'], 'sz.' + df['code'])

    print("\n" + "&" * 99 + "\n")
    df = pd.DataFrame(
        df, columns=["code", "name", "ak_code", "tu_code", "bao_code"])
    today = datetime.now().strftime("%Y%m%d")
    df['date'] = today
    print(df)

    # 存储到SQLite数据库
    df_to_sqlite(df=df, table_name='stock_basic_plus',
                 db_name=db_path, if_exists='replace')
