import QUANTAXIS as QA
import numpy as np
import talib
import pandas as pd
import scipy.signal as signal
import matplotlib.pyplot as plt

# 定义MACD函数
def TA_MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9):
    '''
    参数设置:
        fastperiod = 12
        slowperiod = 26
        signalperiod = 9

    返回: macd - dif, signal - dea, hist * 2 - bar, delta
    '''
    macd, signal, hist = talib.MACD(prices,
                                    fastperiod=fastperiod,
                                    slowperiod=slowperiod,
                                    signalperiod=signalperiod)
    delta = np.r_[np.nan, np.diff(hist * 2)]
    return np.c_[macd, signal, hist * 2, delta]


# 定义MA函数
def TA_MA(prices, timeperiod=5):
    '''
    参数设置:
        timeperiod = 5

    返回: ma
    '''
    ma = talib.MA(prices, timeperiod=timeperiod)
    return ma


# 定义RSI函数
def TA_RSI(prices, timeperiod=12):
    '''
    参数设置:
        timeperiod = 12

    返回: ma
    '''
    rsi = talib.RSI(prices, timeperiod=timeperiod)
    delta = np.r_[np.nan, np.diff(rsi)]
    return np.c_[rsi, delta]


# 定义RSI函数
def TA_BBANDS(prices, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0):
    '''
    参数设置:
        timeperiod = 5
        nbdevup = 2
        nbdevdn = 2

    返回: up, middle, low
    '''
    up, middle, low = talib.BBANDS(prices, timeperiod, nbdevup, nbdevdn, matype)
    ch = (up - low) / low
    delta = np.r_[np.nan, np.diff(ch)]

    ma30 = TA_MA(prices, timeperiod=30)
    boll_band_channel_padding = (ma30 - low) / low
    padding_delta = np.r_[np.nan, np.diff(boll_band_channel_padding)]
    ch_ma30 = talib.MA(ch, timeperiod=30)
    return np.c_[up, middle, low, ch, ch_ma30, delta, padding_delta]


def TA_KDJ(hight, low, close, fastk_period=9, slowk_matype=0, slowk_period=3, slowd_period=3):
    '''
    参数设置:
        fastk_period = 0
        lowk_matype = 0,
        slowk_period = 3,
        slowd_period = 3

    返回: K, D, J
    '''
    K, D = talib.STOCH(hight, low, close, fastk_period=fastk_period, slowk_matype=slowk_matype, slowk_period=slowk_period, slowd_period=slowd_period)
    J = 3 * K - 2 * D
    delta = np.r_[np.nan, np.diff(J)]
    return np.c_[K, D, J, delta]


def TA_CCI(high, low, close, timeperiod=14):
    """
    名称：平均趋向指数的趋向指数
    简介：使用ADXR指标，指标判断ADX趋势。
    """
    real = talib.CCI(high, low, close, timeperiod=14)
    delta = np.r_[np.nan, np.diff(real)]
    return np.c_[real, delta]


# 写个指标 so easy
def ifup20(data):
    # QA内建指标计算 Python原生代码
    return (QA.MA(data.close, 5)-QA.MA(data.close, 20)).dropna() > 0

def ifup20_TA(data):
    # TA-lib计算
    return (TA_MA(data.close, 5)-TA_MA(data.close, 20)).dropna() > 0

# apply到 QADataStruct上

# 写个自定义指标 MAX_FACTOR QA内建指标计算 Python原生代码
def ifmaxfactor_greater(data):
    RSI = QA.QA_indicator_RSI(data)
    CCI = QA.QA_indicator_CCI(data)
    KDJ = QA.QA_indicator_KDJ(data)
    MAX_FACTOR = CCI['CCI'] + (RSI['RSI1'] - 50) * 4 + (KDJ['KDJ_J'] - 50) * 4
    MAX_FACTOR_delta = np.r_[np.nan, np.diff(MAX_FACTOR)]
    REGRESSION_BASELINE = (RSI['RSI1'] - 50) * 4
    return ((MAX_FACTOR+MAX_FACTOR_delta)-(REGRESSION_BASELINE-133)).dropna() > 0

# 写个自定义指标 MAX_FACTOR TA-lib计算
def ifmaxfactor_greater_TA(data):
    RSI = TA_RSI(data.close)
    CCI = TA_CCI(data.high, data.low, data.close)
    KDJ = TA_KDJ(data.high, data.low, data.close)
    MAX_FACTOR = CCI[:,0] + (RSI[:,0] - 50) * 4 + (KDJ[:,2] - 50) * 4
    MAX_FACTOR_delta = np.r_[np.nan, np.diff(MAX_FACTOR)]
    REGRESSION_BASELINE = (RSI[:,0] - 50) * 4
    return pd.DataFrame(((MAX_FACTOR+MAX_FACTOR_delta)-(REGRESSION_BASELINE-133)), index=data.index).dropna() > 0

# 这个函数我一直没法不使用For循环来实现，求天神指导
def Timeline_Integral_with_cross_before(Tm,):
    """
    计算时域金叉/死叉信号的累积卷积和(死叉不清零)
    """
    T = [Tm[0]]
    for i in range(1,len(Tm)):
        if (Tm[i] == 0):
            T.append(T[i - 1] + 1)
        else:
            T.append(0)
    return np.array(T)

if __name__ == '__main__':
    # 获取全市场股票 list格式
    code = QA.QA_fetch_stock_list_adv().code.tolist()
    codelist = QA.QA_fetch_stock_block_adv().get_block('沪深300').code
    #print(codelist[0:30])

    # 获取全市场数据 QADataStruct格式
    data1 = QA.QA_fetch_stock_day_adv(code, '2019-10-01', '2020-02-18')
    data2 = QA.QA_fetch_stock_day_adv(codelist, '2019-10-01', '2020-02-18')

    # %timeit ind1 = data1.add_func(ifup20)
    # %timeit ind1 = data1.add_func(ifup20_TA)
    ind1 = data1.add_func(ifup20_TA)

    # %timeit ind2 = data2.add_func(ifmaxfactor_greater)
    # %timeit ind2 = data2.add_func(ifmaxfactor_greater_TA)
    ind2 = data2.add_func(ifmaxfactor_greater_TA)

    # 对于指标groupby 日期 求和
    ma20_jx_count = ind1.dropna().groupby(level=0).sum() / len(code)
    MAX_FACTOR_jx_count = ind2.dropna().groupby(level=0).sum() / 300

    # 自定义指标极值点查找
    MA20_tp_min, MA20_tp_max = signal.argrelextrema(ma20_jx_count.values, np.less)[0], \
                               signal.argrelextrema(ma20_jx_count.values, np.greater)[0]
    MAX_FACTOR_tp_min, MAX_FACTOR_tp_max = signal.argrelextrema(MAX_FACTOR_jx_count.values, np.less)[0], \
                                           signal.argrelextrema(MAX_FACTOR_jx_count.values, np.greater)[0]

    # 将极值点坐标标记写回 DataFrame 方便画图观察
    ma20_jx_count = pd.DataFrame(ma20_jx_count)
    ma20_jx_count = ma20_jx_count.assign(MA20_TP_CROSS_JX_MARK=None)
    ma20_jx_count.iloc[MA20_tp_min, ma20_jx_count.columns.get_loc('MA20_TP_CROSS_JX_MARK')] = \
    ma20_jx_count.iloc[MA20_tp_min][0]
    ma20_jx_count = ma20_jx_count.assign(MA20_TP_CROSS_SX_MARK=None)
    ma20_jx_count.iloc[MA20_tp_max, ma20_jx_count.columns.get_loc('MA20_TP_CROSS_SX_MARK')] = \
    ma20_jx_count.iloc[MA20_tp_max][0]

    MAX_FACTOR_jx_count = MAX_FACTOR_jx_count.assign(MAX_FACTOR_TP_CROSS_JX_MARK=None)
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_min, MAX_FACTOR_jx_count.columns.get_loc('MAX_FACTOR_TP_CROSS_JX_MARK')] = \
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_min][0]
    MAX_FACTOR_jx_count = MAX_FACTOR_jx_count.assign(MAX_FACTOR_TP_CROSS_SX_MARK=None)
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_max, MAX_FACTOR_jx_count.columns.get_loc('MAX_FACTOR_TP_CROSS_SX_MARK')] = \
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_max][0]

    # 画图
    ma20_jx_count.plot()
    plt.plot(ma20_jx_count['MA20_TP_CROSS_JX_MARK'], 'co')
    plt.plot(ma20_jx_count['MA20_TP_CROSS_SX_MARK'], 'bx')

    plt.plot(MAX_FACTOR_jx_count[0])
    plt.plot(MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_JX_MARK'], 'ro')
    plt.plot(MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_SX_MARK'], 'gx')

    # 利用极值点进行金叉死叉状态和趋势方向判断
    ma20_jx_count = ma20_jx_count.assign(MA20_TP_CROSS_JX=0)
    ma20_jx_count.iloc[MA20_tp_min, ma20_jx_count.columns.get_loc('MA20_TP_CROSS_JX')] = 1
    ma20_jx_count = ma20_jx_count.assign(MA20_TP_CROSS_SX=0)
    ma20_jx_count.iloc[MA20_tp_max, ma20_jx_count.columns.get_loc('MA20_TP_CROSS_SX')] = 1

    MAX_FACTOR_jx_count = MAX_FACTOR_jx_count.assign(MAX_FACTOR_TP_CROSS_JX=0)
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_min, MAX_FACTOR_jx_count.columns.get_loc('MAX_FACTOR_TP_CROSS_JX')] = 1
    MAX_FACTOR_jx_count = MAX_FACTOR_jx_count.assign(MAX_FACTOR_TP_CROSS_SX=0)
    MAX_FACTOR_jx_count.iloc[MAX_FACTOR_tp_max, MAX_FACTOR_jx_count.columns.get_loc('MAX_FACTOR_TP_CROSS_SX')] = 1

    ma20_jx_count['MA20_TP_CROSS_JX'] = Timeline_Integral_with_cross_before(ma20_jx_count['MA20_TP_CROSS_JX'])
    ma20_jx_count['MA20_TP_CROSS_SX'] = Timeline_Integral_with_cross_before(ma20_jx_count['MA20_TP_CROSS_SX'])

    MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_JX'] = Timeline_Integral_with_cross_before(
        MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_JX'])
    MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_SX'] = Timeline_Integral_with_cross_before(
        MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_SX'])

    # 照例，上面的自创指标出现 双金叉，就是买入点信号
    BUY_ACTION = (MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_JX'] < MAX_FACTOR_jx_count['MAX_FACTOR_TP_CROSS_SX'])
    BUY_ACTION2 = (ma20_jx_count['MA20_TP_CROSS_JX'] < ma20_jx_count['MA20_TP_CROSS_SX'])
    BUY_ACTION = BUY_ACTION.tail(len(BUY_ACTION2))
    BUY_ACTION_DUAL = BUY_ACTION & BUY_ACTION2
    BUY_ACTION_DUAL = BUY_ACTION_DUAL[BUY_ACTION_DUAL.apply(lambda x: x == True)]
    ma20_jx_count = ma20_jx_count.assign(DUAL_CROSS_JX_MARK=None)
    ma20_jx_count.loc[BUY_ACTION_DUAL.index, 'DUAL_CROSS_JX_MARK'] = ma20_jx_count.loc[BUY_ACTION_DUAL.index][0]

    # 画图看看
    plt.plot(ma20_jx_count[0])
    plt.plot(MAX_FACTOR_jx_count[0])
    plt.plot(ma20_jx_count['DUAL_CROSS_JX_MARK'], 'ro')




