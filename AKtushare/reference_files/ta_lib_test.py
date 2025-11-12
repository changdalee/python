import akshare as ak
import talib
import pandas as pd

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #对pandas配置，列名与数据对其显示
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    # 显示所有列
    pd.set_option('display.max_columns', None)
    # 显示所有行
    #pd.set_option('display.max_rows', None)

    print('hi PyCharm')

    # 获取A股历史数据
    df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20250708" ,adjust="qfq")
    #print(df)

    #MA（移动平均线）,MA5 上穿 MA20（黄金交叉），买入信号 ✅,MA5 下穿 MA20（死亡交叉），卖出信号 ❌
    df['MA5'] = talib.MA(df['收盘'], timeperiod=5)
    #EMA（指数移动平均线）,比 MA 更灵敏，适合短线交易
    df['EMA5'] = talib.EMA(df['收盘'], timeperiod=5)

    #MACD（指数平滑异同移动平均线）,DIF 上穿 DEA（金叉），买入信号 ✅,DIF 下穿 DEA（死叉），卖出信号 ❌
    df['DIF'], df['DEA'], df['MACD'] = talib.MACD(df['收盘'], fastperiod=12, slowperiod=26, signalperiod=9)

    #布林带（Bollinger Bands）,股价触及上轨，超买，可能回调 📉,股价触及下轨，超卖，可能反弹 📈
    df['upper'], df['middle'], df['lower'] = talib.BBANDS(df['收盘'], timeperiod=20)

    #RSI（相对强弱指数）,RSI > 70，超买，可能下跌 📉,RSI < 30，超卖，可能上涨 📈
    df['RSI'] = talib.RSI(df['收盘'], timeperiod=14)

    #SAR（抛物线转向指标）,SAR 在 K 线上方，趋势看跌 📉,SAR 在 K 线下方，趋势看涨 📈
    df['SAR'] = talib.SAR(df['最高'], df['最低'], acceleration=0.02, maximum=0.2)

    #WILLR（威廉指标）,WILLR < -80，超卖，看涨 📈,WILLR > -20，超买，看跌 📉
    df['WILLR'] = talib.WILLR(df['最高'], df['最低'], df['收盘'], timeperiod=14)

    #MOM（动量指标）,MOM 上升：说明股价上涨动力较强 📈,MOM 下降：说明股价下跌动力较强 📉
    df['MOM'] = talib.MOM(df['收盘'], timeperiod=10)

    #OBV（能量潮指标）,OBV 上升，表示资金流入，股价可能上涨 📈,OBV 下降，表示资金流出，股价可能下跌 📉
    df['OBV'] = talib.OBV(df['收盘'], df['成交量'])

    #ATR（平均真实波幅）,ATR 代表市场的波动性，数值越大，波动越剧烈。,适用于设置止损点，波动大时止损应适当放宽，波动小时止损应收紧
    df['ATR'] = talib.ATR(df['最高'], df['最低'], df['收盘'], timeperiod=14)

    #CDLHAMMER（锤子线）,锤子线：下影线长、实体短，出现在下跌趋势末端，可能反转向上 📈,如果伴随较大的成交量，信号更加强烈！
    df['CDLHAMMER'] = talib.CDLHAMMER(df['开盘'], df['最高'], df['最低'], df['收盘'])

    #CDLDOJI（十字星）,十字星代表市场方向不明，可能意味着趋势反转！,出现在高位，可能下跌 📉,出现在低位，可能上涨 📈,需要结合其他指标进行确认，比如 RSI、MACD。
    df['CDLDOJI'] = talib.CDLDOJI(df['开盘'], df['最高'], df['最低'], df['收盘'])

    #CDLMORNINGSTAR（晨星）,晨星形态：由三根 K 线组成，出现在下跌趋势末端，可能上涨 📈,这个形态是一个强烈的看涨信号，如果配合成交量放大，信号更可靠！
    df['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(df['开盘'], df['最高'], df['最低'], df['收盘'], penetration=0.3)

    # 简单的趋势交易策略：当 MACD 金叉、RSI 低于 30 且 OBV 上升时，考虑买入！ ✅,当 MACD 死叉、RSI 高于 70 且 OBV 下降时，考虑卖出！ ❌
    df['BUY'] = (df['DIF'] > df['DEA']) & (df['RSI'] < 30) & (df['OBV'].diff() > 0)
    df['SELL'] = (df['DIF'] < df['DEA']) & (df['RSI'] > 70) & (df['OBV'].diff() < 0)

    print(df[['日期', 'BUY', 'SELL']].tail())

