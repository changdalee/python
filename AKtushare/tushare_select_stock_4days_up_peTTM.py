# import akshare as ak
import tushare as ts
import baostock as bs
import pandas as pd
import time
import sqlite3
from sqlite3 import OperationalError
from datetime import datetime
import io
import sys
import os


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


def export_to_ths_txt(df, group_name='myselect_stocks'):
    """导出为同花顺TXT格式"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{group_name}_{timestamp}.txt"
    # current_path = os.getcwd()
    current_path = "D:\\"
    filepath = os.path.join(current_path, filename)
    print(f"正在导出文件到: {filepath}...")
    formatted_codes = df["code"]

    # 同花顺标准格式：每行一个6位股票代码
    with open(filepath, 'w', encoding='gbk') as f:  # 重要：使用GBK编码
        f.write('代码    \n')
        for code in formatted_codes:
            f.write(code + '    \n')

    print(f"✅ 成功导出 {len(formatted_codes)} 只股票到: {filepath}")
    return filepath


def robust_query_history_k_data(code, fields, start_date, end_date,
                                frequency="d", adjustflag="3", max_retries=3):
    """带错误处理和重试的查询函数"""

    for attempt in range(max_retries):
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )

            if rs.error_code != '0':
                print(f"第{attempt+1}次尝试失败: {rs.error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # 等待2秒后重试
                    continue
                else:
                    return None

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                return df
            else:
                print(f"未获取到数据: {code}")
                return pd.DataFrame()

        except Exception as e:
            print(f"第{attempt+1}次尝试异常: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            else:
                return None

    return None


def complete_kdata_workflow(stock_codes, start_date, end_date, save_to_file=None):
    """完整的K线数据工作流程"""

    # 登录
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return None

    try:
        # 获取数据
        all_data = []
        for code in stock_codes:
            print(f"正在获取 {code} 的数据...")

            df = robust_query_history_k_data(
                code=code,
                fields="date,code,open,high,low,close,volume,amount,pctChg,turn,peTTM,pbMRQ",
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                all_data.append(df)

        if not all_data:
            print("未获取到任何数据")
            return None

        # 合并数据
        combined_df = pd.concat(all_data, ignore_index=True)

        # 数据清洗和转换
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount',
                           'pctChg', 'turn', 'peTTM', 'pbMRQ']
        for col in numeric_columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df = combined_df.sort_values(
            ['code', 'date']).reset_index(drop=True)

        # 保存到文件
        if save_to_file:
            combined_df.to_csv(save_to_file, index=False, encoding='utf-8-sig')
            print(f"数据已保存到: {save_to_file}")

        print(f"工作流程完成，共处理 {len(combined_df)} 条数据")
        return combined_df

    finally:
        # 退出
        bs.logout()
        print("已退出baostock系统")


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
    db_path = r'D:\develops\python\aktushare.db'
    conn = sqlite3.connect(
        db_path
    )  # 连接数据库:ml-citation{ref="3,6" data="citationList"}
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM stock_days where days <=" + today
    )  # 执行查询:ml-citation{ref="10" data="citationList"}
    rows = cursor.fetchall()  # 获取所有结果:ml-citation{ref="6" data="citationList"}
    conn.close()  # 关闭连接:ml-citation{ref="8" data="citationList"}
    df_days = pd.DataFrame(rows, columns=["days"])

    daybefore1 = df_days["days"].iloc[-1]
    if daybefore1 == today:
        daybefore1 = df_days["days"].iloc[-2]
        daybefore2 = df_days["days"].iloc[-3]
        daybefore3 = df_days["days"].iloc[-4]
        daybefore4 = df_days["days"].iloc[-5]

    else:
        daybefore2 = df_days["days"].iloc[-2]
        daybefore3 = df_days["days"].iloc[-3]
        daybefore4 = df_days["days"].iloc[-4]
    pro = ts.pro_api()
    df_daybf1 = pro.daily(trade_date=daybefore1).fillna(0)
    time.sleep(3)
    df_daybf2 = pro.daily(trade_date=daybefore2).fillna(0)
    time.sleep(3)
    df_daybf3 = pro.daily(trade_date=daybefore3).fillna(0)
    time.sleep(3)
    df_daybf4 = pro.daily(trade_date=daybefore4).fillna(0)
    # print("\n" + "A" * 99 + "\n")
    df_bf1 = df_daybf1[["ts_code", "vol", "close"]]
    df_bf1.columns = ["ts_code", "vol_bf1", "close_bf1"]
    # print(df_bf1)
    df_bf2 = df_daybf2[["ts_code", "vol", "close"]]
    df_bf2.columns = ["ts_code", "vol_bf2", "close_bf2"]
    # print(df_bf2)
    df_bf3 = df_daybf3[["ts_code", "vol", "close"]]
    df_bf3.columns = ["ts_code", "vol_bf3", "close_bf3"]
    # print(df_bf3)
    df_bf4 = df_daybf4[["ts_code", "vol", "close"]]
    df_bf4.columns = ["ts_code", "vol_bf4", "close_bf4"]
    # print(df_bf4)
    # print("\n" + "B" * 99 + "\n")
    df = pd.merge(df_bf1, df_bf2, on="ts_code", how="left")
    df = pd.merge(df, df_bf3, on="ts_code", how="left")
    df = pd.merge(df, df_bf4, on="ts_code", how="left")
    # print(df)
    df.fillna(0, inplace=True)
    # print(df)

    df = df[df["ts_code"].apply(lambda x: not str(x) > "687999.AA")]
    df["code"] = df["ts_code"].apply(lambda x: x[:6])
    # print(df)
    df = df.drop(df[df["vol_bf1"] < 1].index)
    df = df.drop(df[df["vol_bf2"] < 1].index)
    df = df.drop(df[df["vol_bf3"] < 1].index)
    df = df.drop(df[df["vol_bf4"] < 1].index)

    df = df.drop(df[df["close_bf1"] < 1].index)
    df = df.drop(df[df["close_bf2"] < 1].index)
    df = df.drop(df[df["close_bf3"] < 1].index)
    df = df.drop(df[df["close_bf4"] < 1].index)

    # print(df)

    df = df.drop(df[df["vol_bf1"] < df["vol_bf2"]].index)
    df = df.drop(df[df["vol_bf2"] < df["vol_bf3"]].index)
    df = df.drop(df[df["vol_bf3"] < df["vol_bf4"]].index)

    df = df.drop(df[df["close_bf1"] < df["close_bf2"]].index)
    df = df.drop(df[df["close_bf2"] < df["close_bf3"]].index)
    df = df.drop(df[df["close_bf3"] < df["close_bf4"]].index)

    # print(df)

    print("\n" + "_" * 99 + "\n")

    conn = sqlite3.connect(
        db_path
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
    # print(df)

    df = df[
        [
            "code",
            "name",
            "vol_bf1",
            "vol_bf2",
            "vol_bf3",
            "vol_bf4",
            "close_bf1",
            "close_bf2",
            "close_bf3",
            "close_bf4",
        ]
    ]

    df = df.rename(
        columns={
            "code": "code",
            "name": "name",
            "vol_bf1": "vol_bf1",
            "vol_bf2": "vol_bf2",
            "vol_bf3": "vol_bf3",
            "vol_bf4": "vol_bf4",
            "close_bf1": "close_bf1",
            "close_bf2": "close_bf2",
            "close_bf3": "close_bf3",
            "close_bf4": "close_bf4",
        }
    )
    df["bao_code"] = df["code"].apply(
        lambda x: "sh."+x if x.startswith("6") else "sz."+x)
    list_code = df["bao_code"].tolist()
    # 使用完整工作流程
    stock_codes = list_code
    start_date = "2025-11-10"
    end_date = "2025-11-10"

    result_df = complete_kdata_workflow(
        stock_codes=stock_codes,
        start_date=start_date,
        end_date=end_date,
        save_to_file="stock_data.csv"
    )

    if result_df is not None:
        print("\n数据概览:")
        print(result_df.info())
        print(
            f"\n数据时间范围: {result_df['date'].min()} 到 {result_df['date'].max()}")

    df['PE_ratio'] = result_df['peTTM']
    df['date'] = start_date
    df = df.drop(df[df["PE_ratio"] < 1].index)
    df = df.drop(df[df["PE_ratio"] > 20].index)
    print("\n" + "$" * 80 + "\n")
    print(df)
    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name="tushare_select_4days_up_peTTM",
        db_name=db_path,
        if_exists="replace",
    )
    export_to_ths_txt(df)
