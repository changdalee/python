import akshare as ak
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
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
    #对pandas配置，列名与数据对其显示
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    #pd.set_option('display.max_rows', None)

    print_hi('PyCharm')


    #查询所有股票的实时行情数据
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    print(stock_zh_a_spot_em_df)
    
    """
    df = pd.DataFrame(stock_zh_a_spot_em_df)
    na_df = df.fillna(0)  # 填充所有NaN为0
    print(na_df)

    # 只保留成交量大于100万股（1万手）的股票
    df = na_df[na_df['成交量'] > 100000]
    print(df)


    print_hi('PyCharm')
    # 方法1: 直接通过列名列表选择（最常用）
    selected_cols = ['代码', '名称', '今开','最新价','量比','换手率','市盈率-动态','市净率','总市值','流通市值','成交量']
    df1 = df[selected_cols]
    df=df1.rename(columns={'代码': 'code', '名称': 'name', '今开': 'open','最新价':'current','量比':'volume_ratio','换手率':'turnover_ratio','市盈率-动态':'PE_ratio','市净率':'PB_ratio','总市值':'total_capital','流通市值':'trade_capital','成交量':'trade_volume'})

    df = df[df['name'].apply(lambda x: 'ST' not in str(x) and '*ST' not in str(x) and 'PT' not in str(x)
                                       and '退' not in str(x) )]

    df = df[df['code'].apply(lambda x: not str(x)>'687999')]
    
    # lst=['ST','PT','退']
    #df = df[~df['name'].isin(lst)]
    print(df)
    print("\n" + "&" * 80 + "\n")

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='stock_zh_a_cleaned',
        db_name='akshare.db',
        if_exists='replace'
    )

    """
'''
    today = datetime.now().strftime("%Y%m%d")
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='stock_PB_PE'+today,
        db_name='akshare.db',
        if_exists='replace'
    )
    '''