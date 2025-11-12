# This is a sample Python script.

# Press Ctrl+F5 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
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



    #查询沪市A股股票清单
    #stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    #print(stock_sh_a_spot_em_df)
    # 查询深市A股股票清单
    #stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    #print(stock_sz_a_spot_em_df)
    # 查询京市A股股票清单
    #stock_bj_a_spot_em_df = ak.stock_bj_a_spot_em()
    #print(stock_bj_a_spot_em_df)
    #查询所有股票的实时行情数据
    #stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    #print(stock_zh_a_spot_em_df)

    #查询个股信息
    #stock_individual_basic_info_xq_df = ak.stock_individual_basic_info_xq(symbol="SH600650")
    #print(stock_individual_basic_info_xq_df)
    # 查询单个股票的实时行情数据
    #stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="002714", period="daily", start_date="20240101", end_date='20250705',adjust="")
    #print(stock_zh_a_hist_df)
    #新浪高质量数据接口，无访问限制
    #stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol="sz000001", start_date="20250101", end_date="20250704", adjust="qfq")
    #print(stock_zh_a_daily_qfq_df)
    #查询次新股数据
    #stock_zh_a_new_df = ak.stock_zh_a_new()
    #print(stock_zh_a_new_df)
    #查询新股数据
    #stock_zh_a_new_em_df = ak.stock_zh_a_new_em()
    #print(stock_zh_a_new_em_df)
    #查询A+H两地上市股票清单
    #stock_zh_ah_name_df = ak.stock_zh_ah_name()
    #print(stock_zh_ah_name_df)
    #科技类知名美股清单
    #stock_us_famous_spot_em_df = ak.stock_us_famous_spot_em(symbol='科技类')
    #print(stock_us_famous_spot_em_df)
    #上市公司股票质押情况
    #stock_gpzy_pledge_ratio_em_df = ak.stock_gpzy_pledge_ratio_em(date="20250704")
    #print(stock_gpzy_pledge_ratio_em_df)
    #主要股东质押情况
    #stock_gpzy_pledge_ratio_detail_em_df = ak.stock_gpzy_pledge_ratio_detail_em()
    #print(stock_gpzy_pledge_ratio_detail_em_df)
    #上市公司股权质押比例
    #stock_gpzy_industry_data_em_df = ak.stock_gpzy_industry_data_em()
    #print(stock_gpzy_industry_data_em_df)
    #股票账户数统计
    #stock_account_statistics_em_df = ak.stock_account_statistics_em()
    #print(stock_account_statistics_em_df)
    #沪深港通资金流动情况
    #stock_hsgt_fund_flow_summary_em_df = ak.stock_hsgt_fund_flow_summary_em()
    #print(stock_hsgt_fund_flow_summary_em_df)
    #主力资金流入情况，symbol="全部股票"；choice of {"全部股票", "沪深A股", "沪市A股", "科创板", "深市A股", "创业板", "沪市B股", "深市B股"}
    #stock_main_fund_flow_df = ak.stock_main_fund_flow(symbol="全部股票")
    #print(stock_main_fund_flow_df)
    #股东户数统计
    #stock_zh_a_gdhs_df = ak.stock_zh_a_gdhs(symbol="20241231")
    #print(stock_zh_a_gdhs_df)
    #A股股票列表
    #stock_info_a_code_name_df = ak.stock_info_a_code_name()
    #print(stock_info_a_code_name_df)
    #次新股票池
    #stock_zt_pool_sub_new_em_df = ak.stock_zt_pool_sub_new_em(date='20250704')
    #print(stock_zt_pool_sub_new_em_df)
    #创月新高股票，symbol="创月新高"; choice of {"创月新高", "半年新高", "一年新高", "历史新高"}
    #stock_rank_cxg_ths_df = ak.stock_rank_cxg_ths(symbol="历史新高")
    #print(stock_rank_cxg_ths_df)
    #df=stock_rank_cxg_ths_df
    #创月新低股票，symbol="创月新低"; choice of {"创月新低", "半年新低", "一年新低", "历史新低"}
    #stock_rank_cxd_ths_df = ak.stock_rank_cxd_ths(symbol="半年新低")
    #print(stock_rank_cxd_ths_df)
    #df=stock_rank_cxd_ths_df
    #技术选股-连续上涨
    #stock_rank_lxsz_ths_df = ak.stock_rank_lxsz_ths()
    #print(stock_rank_lxsz_ths_df)
    # 技术选股-连续下跌
    #stock_rank_lxxd_ths_df = ak.stock_rank_lxxd_ths()
    #print(stock_rank_lxxd_ths_df)
    # 技术选股-持续放量
    #stock_rank_cxfl_ths_df = ak.stock_rank_cxfl_ths()
    #print(stock_rank_cxfl_ths_df)
    #df=stock_rank_cxfl_ths_df.fillna(0)
    # 技术选股-持续缩量
    #stock_rank_cxsl_ths_df = ak.stock_rank_cxsl_ths()
    #print(stock_rank_cxsl_ths_df)
    # 技术选股-向上突破，symbol="500日均线"; choice of {"5日均线", "10日均线", "20日均线", "30日均线", "60日均线", "90日均线", "250日均线", "500日均线"}
    #stock_rank_xstp_ths_df = ak.stock_rank_xstp_ths(symbol="500日均线")
    #print(stock_rank_xstp_ths_df)
    # 技术选股-向下突破，symbol="500日均线"; choice of {"5日均线", "10日均线", "20日均线", "30日均线", "60日均线", "90日均线", "250日均线", "500日均线"}
    #stock_rank_xxtp_ths_df = ak.stock_rank_xxtp_ths(symbol="500日均线")
    #print(stock_rank_xxtp_ths_df)
    #技术选股-量价齐升
    #stock_rank_ljqs_ths_df = ak.stock_rank_ljqs_ths()
    #print(stock_rank_ljqs_ths_df)
    #技术选股-量价齐跌
    #stock_rank_ljqd_ths_df = ak.stock_rank_ljqd_ths()
    #print(stock_rank_ljqd_ths_df)
    #技术选股 - 险资举牌
    #stock_rank_xzjp_ths_df = ak.stock_rank_xzjp_ths()
    #print(stock_rank_xzjp_ths_df)
    #新浪财经-ESG评级数据
    #stock_esg_rate_sina_df = ak.stock_esg_rate_sina()
    #print(stock_esg_rate_sina_df)
    #新浪财经-MSCI评级数据
    #stock_esg_msci_sina_df = ak.stock_esg_msci_sina()
    #print(stock_esg_msci_sina_df)
    # 新浪财经-路孚特评级数据
    #stock_esg_rft_sina_df = ak.stock_esg_rft_sina()
    #print(stock_esg_rft_sina_df)
    # 新浪财经-秩鼎评级数据
    #stock_esg_zd_sina_df = ak.stock_esg_zd_sina()
    #print(stock_esg_zd_sina_df)
    # 新浪财经-华证指数数据
    #stock_esg_hz_sina_df = ak.stock_esg_hz_sina()
    #print(stock_esg_hz_sina_df)
    #交易日历数据
    #tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    #print(tool_trade_date_hist_sina_df)
    #中国各大城市-日出和日落时间, 数据区间从 19990101-至今, 推荐使用代理访问
    #sunrise_daily_df = ak.sunrise_daily(date="20240816", city="tianjin")
    #print(sunrise_daily_df)
    #加密货币（比特币）实时行情，该功能已经停止更新
    #crypto_js_spot_df = ak.crypto_js_spot()
    #print(crypto_js_spot_df)

    '''
    df = pd.DataFrame(stock_rank_cxfl_ths_df)
    # 方法1: 直接通过列名列表选择（最常用）
    selected_cols = ['股票代码', '股票简称', '最新价']
    df1 = df[selected_cols]

    df_cleaned = df1[~df1['股票简称'].str.contains('ST', na=False)]
    df = df_cleaned[~df_cleaned['股票简称'].str.contains('退', na=False)]
    df = df[~df['股票简称'].str.contains('PT', na=False)]

    print("方法1 - 选择指定列名:")
    print(df)
    print("\n" + "_" * 80 + "\n")
    
    # 存储到SQLite数据库
    df_to_sqlite(
        df=stock_individual_basic_info_xq_df,
        table_name='stock_rank_cxfl_cleaned',
        db_name='akshare.db',
        if_exists='replace'
    )
    '''