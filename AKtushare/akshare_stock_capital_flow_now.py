import tushare as ts
import akshare as ak
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
import time
from datetime import date, timedelta, datetime
# from sqlalchemy import types


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
        c.execute("DELETE from {table_name};")
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


if __name__ == '__main__':
    # 对pandas配置，列名与数据对其显示
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)

    token = '055680ead4592f1287876ef50197e46a76516c86268a33b8c0c565b0'
    ts.set_token(token)
    # print(ts.__version__)

    print_hi('PyCharm')

    # 同花顺-数据中心-资金流向-个股资金流,symbol="即时"; choice of {“即时”, "3日排行", "5日排行", "10日排行", "20日排行"}

    '''
    conn = sqlite3.connect('akshare.db')  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM akshare_stock_capital_flow_20250722")  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df = pd.DataFrame(rows,columns=['序号','股票代码','股票简称','最新价','涨跌幅','换手率','流入资金','流出资金','净额','成交额','数字净额'])
    '''
    # symbol = "即时";    choiceof{“即时”, "3日排行", "5日排行", "10日排行", "20日排行"}
    df = ak.stock_fund_flow_individual(symbol="即时")
    df['数字净额'] = 0
    # df['股票代码'] = df['股票代码'].astype('str')
    print(df)
    print('-------------------------%%%%%%%%%%%%%%%%%%%%%%%%%%%------------------------------')
    max_len = len(df)
    # print(max_len)
    count = 0
    while count < max_len:
        if df.iloc[count, 8].endswith('万'):
            df.iloc[count, 10] = round(
                float(df.iloc[count, 8].removesuffix('万'))*10000, 1)
        elif df.iloc[count, 8].endswith('亿'):
            df.iloc[count, 10] = round(
                float(df.iloc[count, 8].removesuffix('亿'))*10000*10000, 1)
        else:
            df.iloc[count, 10] = round(float(df.iloc[count, 8]), 1)
        count += 1

    # df['code'] = df['股票代码'].apply(lambda x: x[:6])
    df = df.drop(df[df['数字净额'] < 20000000].index)
    df = df[df['股票简称'].apply(lambda x: 'ST' not in str(x) and '*ST' not in str(x) and 'PT' not in str(x)
                                       and '退' not in str(x))]

    df = df[df['股票代码'].apply(lambda x: not str(x) > '687999')]
    print(df)

    print("\n" + "_" * 88 + "\n")
    today = datetime.now().strftime("%Y%m%d")

    # 存储到SQLite数据库
    '''
    dtype_mapping = {
        '序号':types.Integer(),
        '股票代码':types.Text(),
        '股票简称':types.Text(),
        '最新价':types.Float(),
        '涨跌幅':types.Float(),
        '换手率':types.Float(),
        '流入资金':types.Text(),
        '流出资金':types.Text(),
        '净额':types.Text(),
        '成交额':types.Text(),
        '数字净额':types.Float()
    }
    '''
    # ['序号', '股票代码', '股票简称', '最新价', '涨跌幅', '换手率', '流入资金', '流出资金', '净额', '成交额','数字净额'])
    df_to_sqlite(
        df=df,
        table_name='akshare_stock_capital_flow_'+today,
        db_name='akshare.db',
        # dtype=dtype_mapping,
        if_exists='replace'
    )
