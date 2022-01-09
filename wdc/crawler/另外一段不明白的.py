def get_peak_high_price(logVs):
    logHs = [logVs[0]]
    for logV in logVs[1:]:
        if logV>logHs[-1]:
            logHs.append(logV)
        else:
            logHs.append(logHs[-1])
    logHs = np.array(logHs)
    return logHs


def getPoolRate(logVs):
    """
    # 对数积水率
    """
    logHs = get_peak_high_price(logVs)
    gain = np.sum(logVs) - logVs[0] * len(logVs)
    lost_and_gain = np.sum(logHs) - logHs[0] * len(logHs)
    ninfp1_to_n1p1 = lambda x: 2 / (2 - x) - 1
    poolRate = None
    if gain == 0:
        poolRate = 0
    elif lost_and_gain == 0:
        poolRate = float('-inf')
    else:
        poolRate = gain / lost_and_gain
    poolRate = ninfp1_to_n1p1(poolRate)
    return poolRate


def getPoolRate_vX(logVs,
                   regression,):
    """
    # 稳健对数积水率
    """
    logHs = get_peak_high_price(logVs)
    gain = np.sum(logVs) - logVs[0] * len(logVs)
    lost_and_gain = np.sum(logHs) - logHs[0] * len(logHs)

    lost = lost_and_gain - gain

    dts = np.arange(len(logVs))
    #s, m = get_coef_cut(dts, logVs)
    #predLogVs = s * dts + m
    #gain = (predLogVs[-1] - predLogVs[0]) * len(logVs) / 2
    gain = (regression[-1] - regression[0]) * len(logVs) / 2
    lost_and_gain = np.abs(lost + gain)
    ninfp1_to_n1p1 = lambda x: 2 / (2 - x) - 1

    poolRate = None
    if gain == 0:
        poolRate = 0
    elif lost_and_gain == 0:
        poolRate = float('-inf')
    else:
        poolRate = gain / lost_and_gain

    poolRate = ninfp1_to_n1p1(poolRate)
    return poolRate

ret_concept_kline = get_stock_concept_kline(['BK0733'])

ret_concept_kline = get_stock_concept_kline(['BK0437'])
print(ret_concept_kline)
ret_concept_np = ret_concept_kline[[AKA.HIGH]].values

from GolemQ.fractal.v0 import (
    ma30_cross_func,
    renko_trend_cross_func,
    zen_trend_func,
)

features = ma30_cross_func(ret_concept_kline)
features = renko_trend_cross_func(ret_concept_kline, features)
features = zen_trend_func(ret_concept_kline, features)
from GolemQ.fractal.v8 import (
    calc_zen_chopsticks,
)

features = calc_zen_chopsticks(ret_concept_kline,
                               features,
                               detect_buy3=False)

ret_fractal_length = int(np.maximum(features[FLD.DEA_ZERO_TIMING_LAG],
                                    np.maximum(features[FLD.MA90_CLEARANCE_TIMING_LAG],
                                               np.maximum(features[FLD.ZEN_PEAK_TIMING_LAG],
                                                          features[FLD.MA90_TREND_TIMING_LAG])))[-1])

print(ret_fractal_length)

ret_inter = getPoolRate(ret_concept_np[-ret_fractal_length:])
ret_inter_vX = getPoolRate_vX(ret_concept_np[-ret_fractal_length:],
                              features[FLD.LINEAREG_PRICE].values[-ret_fractal_length:])

from GolemQ.fetch.kline import (
    get_kline_price,
    get_kline_price_v2,
    get_kline_price_min,
)

start = '{}'.format(dt.now() - timedelta(days=1680))
data_day, stock_name_faked = get_kline_price('300821', start=start, verbose=True)
from GolemQ.fractal.v0 import (
    ma30_cross_func,
    renko_trend_cross_func,
    zen_trend_func,
)

features = ma30_cross_func(data_day.data)
features = renko_trend_cross_func(data_day.data, features)
features = zen_trend_func(data_day.data, features)
features = calc_zen_chopsticks(data_day.data,
                               features,
                               detect_buy3=False)

ret_fractal_length = int(np.maximum(features[FLD.DEA_ZERO_TIMING_LAG],
                                    np.maximum(features[FLD.MA90_CLEARANCE_TIMING_LAG],
                                               np.maximum(features[FLD.ZEN_PEAK_TIMING_LAG],
                                                          features[FLD.MA90_TREND_TIMING_LAG])))[-1])
from GolemQ.fetch.kline import (
    get_kline_price,
    get_kline_price_v2,
    get_kline_price_min,
)

start = '{}'.format(dt.now() - timedelta(days=1680))
data_day, stock_name_faked = get_kline_price('300821', start=start, verbose=True)
from GolemQ.fractal.v0 import (
    ma30_cross_func,
    renko_trend_cross_func,
    zen_trend_func,
)

features = ma30_cross_func(data_day.data)
features = renko_trend_cross_func(data_day.data, features)
features = zen_trend_func(data_day.data, features)
features = calc_zen_chopsticks(data_day.data,
                               features,
                               detect_buy3=False)

ret_fractal_length = int(np.maximum(features[FLD.DEA_ZERO_TIMING_LAG],
                                    np.maximum(features[FLD.MA90_CLEARANCE_TIMING_LAG],
                                               np.maximum(features[FLD.ZEN_PEAK_TIMING_LAG],
                                                          features[FLD.MA90_TREND_TIMING_LAG])))[-1])
print(ret_fractal_length)
ret_inter = getPoolRate(data_day.data[AKA.HIGH].values[-ret_fractal_length:])
ret_inter_vX = getPoolRate_vX(data_day.data[AKA.HIGH].values[-ret_fractal_length:],
                              features[FLD.LINEAREG_PRICE].values[-ret_fractal_length:])