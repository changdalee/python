import akshare as ak
import pandas as pd
import sqlite3
from sqlite3 import OperationalError

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


    #获取沪深港通资金流向数据
    stock_hsgt_fund_flow_summary_em_df = ak.stock_hsgt_fund_flow_summary_em()
    print(stock_hsgt_fund_flow_summary_em_df)

    df = pd.DataFrame(stock_hsgt_fund_flow_summary_em_df)
    na_df = df.fillna(0)  # 填充所有NaN为0
    #print(na_df)
    df=na_df
    print(df)
    print("\n" + "_" * 80 + "\n")

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='stock_hsgt_fund_flow_summary',
        db_name='akshare.db',
        if_exists='replace'
    )
