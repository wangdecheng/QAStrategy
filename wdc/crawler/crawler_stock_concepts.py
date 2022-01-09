from random import randint
import sys
import json
import requests
from requests.exceptions import ConnectTimeout, SSLError, ReadTimeout, ConnectionError
import numpy as np
import pandas as pd
from GolemQ.fetch.crawler import (
    headers,
)
from GolemQ.utils.parameter import (
    AKA,
    INDICATOR_FIELD as FLD,
    TREND_STATUS as ST,
    FEATURES as FTR,
)
import QUANTAXIS as QA
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_code_tolist,
    QA_util_date_stamp,
    QA_util_date_valid,
    QA_util_log_info,
)
from QUANTAXIS.QAUtil import (
    QA_util_to_json_from_pandas
)
from QUANTAXIS.QAUtil.QADate_trade import (
    QA_util_get_pre_trade_date,
    QA_util_if_tradetime,
)
import time
from datetime import (
    datetime as dt,
    timezone, timedelta
)
import datetime
import pymongo
import traceback
from tqdm.notebook import trange, tqdm


def GQ_fetch_stock_concept_components(concept='BK0066',
                                      delay_gap=0.8,
                                      pagenumber=1,
                                      pagelimit=400,
                                      concept_name=None, ):
    '''
    抓取东方财富板块概念成分数据
    http://28.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112405892331704393758_1629080255227&pn=2&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:BK0981+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45&_=1629080255234
    '''

    def get_random_stock_concept_components_url():
        url = "http://{:2d}.push2.eastmoney.com/api/qt/clist/get".format(randint(1, 99))
        return url

    params = {
        "pn": pagenumber,
        "pz": pagelimit,
        "po": '1',
        "np": '1',
        "fltt": '2',
        "invt": '2',
        'fid': 'f3',
        'fs': 'b:{:s}+f:!50'.format(concept),
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "cb": "jQuery1124023986497915529914_{:d}".format(int(dt.utcnow().timestamp())),
        "_": int(time.time() * 1000),
    }

    retries = 1
    while (retries != 0):
        try:
            url = get_random_stock_concept_components_url()
            r = requests.get(url, params=params, headers=headers)
            retries = 0
        except (ConnectTimeout, ConnectionError, SSLError, ReadTimeout):
            retries = retries + 1
            if (retries % 18 == 0):
                print("Retry {} #{}".format(retries - 1))
            time.sleep(delay_gap)
    text_data = r.text
    json_data = json.loads(text_data[text_data.find("{"): -2])
    # print(concept, json_data)
    if (isinstance(json_data, dict)):
        if (json_data["data"] is None):
            return None
        try:
            content_list = json_data["data"]["diff"]
        except Exception as e:
            print(type(json_data))
            print(json_data)
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(u'Failed:{}\n'.format(stock), e)
            return None
        # print(json_data)

        temp_df = pd.DataFrame([item for item in content_list])
        # print(temp_df)
        ret_stock_concept_components = temp_df.rename(columns={'f2': AKA.CLOSE,
                                                               'f3': FLD.PCT_CHANGE,
                                                               'f4': 'delta',
                                                               'f12': AKA.CODE,
                                                               'f14': AKA.NAME})
        ret_stock_concept_components['concept_symbol'] = concept
        ret_stock_concept_components['concept_name'] = concept_name
        ret_stock_concept_components[FLD.PCT_CHANGE] = np.where(ret_stock_concept_components[FLD.PCT_CHANGE] == '-',
                                                                0.0, ret_stock_concept_components[FLD.PCT_CHANGE])
        ret_stock_concept_components['delta'] = np.where(ret_stock_concept_components['delta'] == '-',
                                                         0.0, ret_stock_concept_components['delta'])
        ret_stock_concept_components[AKA.CLOSE] = np.where(ret_stock_concept_components[AKA.CLOSE] == '-',
                                                           0.0, ret_stock_concept_components[AKA.CLOSE])
        ret_stock_concept_components[FLD.PCT_CHANGE] = ret_stock_concept_components[FLD.PCT_CHANGE] / 100
        ret_stock_concept_components[FLD.SOURCE] = 'eastmoney'
        # print( ret_stock_concept_components)
        return ret_stock_concept_components

    return


def fetch_stock_concept_list_from_eastmoney(delay_gap=0.8,
                                            pagenumber=1,
                                            pagelimit=400,
                                            model='concept'):
    '''
    抓取东方财富板块概念数据
    http://76.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124023986497915529914_1628836522452&pn=1&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222&_=1628836522455
    http://58.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406314256519339916_1629966378474&pn=1&pz=400&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222&_=1629966378479
    '''

    def get_random_stock_concept_url():
        url = "http://{:2d}.push2.eastmoney.com/api/qt/clist/get".format(randint(1, 99))
        return url

    params = {
        "pn": pagenumber,
        "pz": pagelimit,
        "np": '1',
        "fltt": '2',
        "invt": '2',
        'fid': 'f3',
        'fs': 'm:90+t:3+f:!50' if (model == 'concept') else 'm:90+t:2+f:!50',
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "cb": "jQuery1124023986497915529914_{:d}".format(int(dt.utcnow().timestamp())),
        "_": int(time.time() * 1000),
    }

    retries = 1
    while (retries != 0):
        try:
            url = get_random_stock_concept_url()
            r = requests.get(url, params=params, headers=headers)
            retries = 0
        except (ConnectTimeout, ConnectionError, SSLError, ReadTimeout):
            retries = retries + 1
            if (retries % 18 == 0):
                print("Retry {} #{}".format(retries - 1))
            time.sleep(delay_gap)
    text_data = r.text
    json_data = json.loads(text_data[text_data.find("{"): -2])
    # print(stock_bk, json_data)
    if (isinstance(json_data, dict)):
        if (json_data["data"] is None):
            return None
        try:
            content_list = json_data["data"]["diff"]
        except Exception as e:
            print(type(json_data))
            print(json_data)
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(u'Failed:{}\n'.format(stock), e)
            return None
        # print(json_data)

        temp_df = pd.DataFrame([item for item in content_list])
        # print(temp_df)
        ret_stock_concept_list = temp_df.rename(columns={'f2': AKA.CLOSE,
                                                         'f3': FLD.PCT_CHANGE,
                                                         'f4': 'delta',
                                                         'f12': AKA.SYMBOL,
                                                         'f14': AKA.NAME})
        ret_stock_concept_list[FLD.PCT_CHANGE] = np.where(ret_stock_concept_list[FLD.PCT_CHANGE] == '-',
                                                          0.0, ret_stock_concept_list[FLD.PCT_CHANGE])
        ret_stock_concept_list['delta'] = np.where(ret_stock_concept_list['delta'] == '-',
                                                   0.0, ret_stock_concept_list['delta'])
        ret_stock_concept_list[FLD.PCT_CHANGE] = ret_stock_concept_list[FLD.PCT_CHANGE] / 100
        ret_stock_concept_list[FLD.SOURCE] = 'eastmoney'
        ret_stock_concept_list[FLD.MODEL] = 'concept' if (model == 'concept') else 'industry'
        return ret_stock_concept_list


def GQ_save_stock_concept_kline(stock_concept_kline_df=None,
                                freq=QA.FREQUENCE.HOUR,
                                collections=DATABASE.stock_concept_min):
    """
    东方财富股票概念板块K线数据，将其保存在数据库
    """
    if (freq == QA.FREQUENCE.DAY):
        collections = DATABASE.stock_concept_day
        coll = DATABASE.stock_concept_day
        coll.create_index([(AKA.SYMBOL,
                            pymongo.ASCENDING),
                           ("date_stamp",
                            pymongo.ASCENDING)],
                          unique=True)
    else:
        collections = DATABASE.stock_concept_min
        coll = DATABASE.stock_concept_min
        coll.create_index([(AKA.SYMBOL,
                            pymongo.ASCENDING),
                           ("type",
                            pymongo.ASCENDING),
                           (FLD.DATETIME,
                            pymongo.ASCENDING), ],
                          unique=False)
        coll.create_index([(AKA.SYMBOL,
                            pymongo.ASCENDING),
                           ("type",
                            pymongo.ASCENDING),
                           ("time_stamp",
                            pymongo.ASCENDING), ],
                          unique=True)
        coll.create_index([(AKA.SYMBOL,
                            pymongo.ASCENDING),
                           ("type",
                            pymongo.ASCENDING),
                           ("date_stamp",
                            pymongo.ASCENDING), ],
                          unique=False)

    data = stock_concept_kline_df

    # 查询是否新数据
    if (freq == QA.FREQUENCE.DAY):
        query_id = {
            AKA.SYMBOL: data.iloc[0].symbol,
            'date_stamp': {
                '$in': data['date_stamp'].tolist()
            }
        }
    else:
        query_id = {
            AKA.SYMBOL: data.iloc[0].symbol,
            'type': freq,
            'time_stamp': {
                '$in': data['time_stamp'].tolist()
            }
        }
    refcount = coll.count_documents(query_id)

    try:
        if refcount > 0:
            if (len(data) > 1):
                # 删掉重复数据
                coll.delete_many(query_id)
                data = QA_util_to_json_from_pandas(data)
                coll.insert_many(data)
            else:
                # 持续接收行情，更新记录
                if ('created_at' in data.columns):
                    data.drop('created_at', axis=1, inplace=True)
                data = QA_util_to_json_from_pandas(data)
                coll.replace_one(query_id, data[0])
        else:
            # 新 tick，插入记录
            data = QA_util_to_json_from_pandas(data)
            coll.insert_many(data)

    except Exception as e:
        if (data is not None):
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(u'Failed:{}\n'.format(code), e)

    return


def GQ_save_stock_concept_components(stock_concept_components_df=None,
                                     collections=DATABASE.stock_concept_components):
    """
    获取东方财富股票概念板块成分数据，并将其保存在数据库
    """
    coll = collections
    coll.create_index([('concept_symbol',
                        pymongo.ASCENDING),
                       (AKA.CODE,
                        pymongo.ASCENDING),
                       ("revision",
                        pymongo.ASCENDING)],
                      unique=True)
    coll.create_index([('concept_symbol',
                        pymongo.ASCENDING),
                       (AKA.NAME,
                        pymongo.ASCENDING),
                       ("revision",
                        pymongo.ASCENDING)],
                      unique=True)
    coll.create_index([(AKA.CODE,
                        pymongo.ASCENDING),
                       ("revision",
                        pymongo.ASCENDING)],
                      unique=False)

    column_list = [AKA.CLOSE, FLD.PCT_CHANGE, 'delta', ]
    stock_concept_components_df.loc[:, column_list] = stock_concept_components_df[column_list].astype(np.float64)
    print(stock_concept_components_df)

    date_epoch = dt.now().date()
    if not QA_util_if_tradetime(dt.now()):
        date_epoch = QA_util_get_pre_trade_date('{}'.format(datetime.date.today()), n=0)

    stock_concept_components_df['date'] = pd.to_datetime(date_epoch)
    stock_concept_components_df['revision'] = pd.to_datetime(stock_concept_components_df['date']).astype(
        np.int64) // 10 ** 9
    stock_concept_components_df['date_stamp'] = pd.to_datetime(stock_concept_components_df['date']).astype(
        np.int64) // 10 ** 9

    data = stock_concept_components_df
    # print((each_day[-1], slice(None)))
    # 查询是否新数据
    query_id = {
        'concept_symbol': data['concept_symbol'].tail(1).item(),
        AKA.CODE: {
            '$in': data[AKA.CODE].tolist()
        },
        "revision": int(data.tail(1).date_stamp),
    }
    refcount = coll.count_documents(query_id)

    if refcount > 0:
        if (len(data) > 1):
            # 删掉重复数据
            coll.delete_many(query_id)

            # 作为差量更新，只更新最后一天的数据
            data = QA_util_to_json_from_pandas(data)
            coll.insert_many(data)
        else:
            # 持续接收行情，更新记录
            if ('created_at' in data.columns):
                data.drop('created_at', axis=1, inplace=True)
            data = QA_util_to_json_from_pandas(data)
            coll.replace_one(query_id, data[0])
    else:
        # 新 tick，插入记录
        try:
            data = QA_util_to_json_from_pandas(data)
            coll.insert_many(data)
        except Exception as e:
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(e)
            return None

    return stock_concept_components_df


def GQ_SU_crawl_stock_concept_from_eastmoney(
        collections=DATABASE.stock_concept_list,
        model='concept'):
    """
    获取东方财富股票概念板块数据，并将其保存在数据库
    """
    stock_concept_list_df = fetch_stock_concept_list_from_eastmoney(model=model)[[AKA.CLOSE,
                                                                                  FLD.PCT_CHANGE,
                                                                                  'delta',
                                                                                  AKA.SYMBOL,
                                                                                  AKA.NAME,
                                                                                  FLD.SOURCE]].copy()

    coll = collections
    coll.create_index([(AKA.SYMBOL,
                        pymongo.ASCENDING),
                       ("revision",
                        pymongo.ASCENDING)],
                      unique=True)

    column_list = [AKA.CLOSE, FLD.PCT_CHANGE, 'delta', ]
    stock_concept_list_df.loc[:, column_list] = stock_concept_list_df[column_list].astype(np.float64)

    date_epoch = dt.now().date()
    if not QA_util_if_tradetime(dt.now()):
        date_epoch = QA_util_get_pre_trade_date('{}'.format(datetime.date.today()), n=0)

    stock_concept_list_df['date'] = pd.to_datetime(date_epoch)
    stock_concept_list_df['revision'] = pd.to_datetime(stock_concept_list_df['date']).astype(np.int64) // 10 ** 9
    stock_concept_list_df['date_stamp'] = pd.to_datetime(stock_concept_list_df['date']).astype(np.int64) // 10 ** 9

    data = stock_concept_list_df
    # print((each_day[-1], slice(None)))
    # 查询是否新数据
    query_id = {
        AKA.SYMBOL: {
            '$in': data[AKA.SYMBOL].tolist()
        },
        "revision": int(data.tail(1).date_stamp),
    }
    refcount = coll.count_documents(query_id)

    if refcount > 0:
        if (len(data) > 1):
            # 删掉重复数据
            coll.delete_many(query_id)

            # 作为差量更新，只更新最后一天的数据
            data = QA_util_to_json_from_pandas(data)
            coll.insert_many(data)
        else:
            # 持续接收行情，更新记录
            if ('created_at' in data.columns):
                data.drop('created_at', axis=1, inplace=True)
            data = QA_util_to_json_from_pandas(data)
            coll.replace_one(query_id, data[0])
    else:
        # 新 tick，插入记录
        try:
            data = QA_util_to_json_from_pandas(data)
            coll.insert_many(data)
        except Exception as e:
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(e)
            return None

    return stock_concept_list_df


def GQ_SU_crawl_stock_concept_components_from_eastmoney(concept="all",
                                                        delay_gap=0.8,
                                                        pagenumber=1,
                                                        pagelimit=400,
                                                        collections=DATABASE.stock_concept_components):
    """
    获取东方财富股票概念板块数据，并将其保存在数据库
    """
    stock_concept_list = GQ_SU_crawl_stock_concept_from_eastmoney(model='concept')
    stock_industry_list = GQ_SU_crawl_stock_concept_from_eastmoney(model='industry')
    stock_concept_list = pd.concat([stock_concept_list,
                                    stock_industry_list], axis=0)

    coll_components = collections



    if concept == "all":
        # 读取东方财富Choice数据的板块股票列表代码
        print(u"💪 一共需要获取 %d 个概念板块的成分股列表 , 需要大概 %d 分钟" % (len(stock_concept_list),
                                                          (len(stock_concept_list) * delay_gap * 1.3) / 60))

        code_list = stock_concept_list[AKA.SYMBOL].to_list()
        code_metadata = {each_code: "fooo" for each_code in stock_concept_list[AKA.SYMBOL].to_list()}
        try:
            overall_progress = tqdm(code_list, unit='stock')
            for code in overall_progress:
                overall_progress.set_description(u"概念板块({})".format(code))
                overall_progress.update(1)
                try:
                    stock_concept_block = code
                    code_metadata = stock_concept_list.query('{}==\'{}\''.format(AKA.SYMBOL, code))
                    stock_concept_components_df = crawl_stock_concept_components(code, code_metadata)[[AKA.CLOSE,
                                                                                                       FLD.PCT_CHANGE,
                                                                                                       'delta',
                                                                                                       AKA.CODE,
                                                                                                       AKA.NAME,
                                                                                                       'concept_symbol',
                                                                                                       'concept_name',
                                                                                                       FLD.SOURCE]].copy()

                    if (stock_concept_components_df is None):
                        continue

                    data = stock_concept_components_df
                    save_stock_concept_components(code,
                                                  data, )

                    ret_concept_kline_hour = fetch_stock_concept_from_eastmoney(stock_bk=code,
                                                                                freq=QA.FREQUENCE.HOUR, )

                    if (ret_concept_kline_hour is None):
                        continue

                    GQ_save_stock_concept_kline(ret_concept_kline_hour,
                                                freq=QA.FREQUENCE.HOUR)

                    ret_concept_kline_min = fetch_stock_concept_from_eastmoney(stock_bk=code,
                                                                               freq=QA.FREQUENCE.FIFTEEN_MIN, )

                    if (ret_concept_kline_min is None):
                        continue

                    GQ_save_stock_concept_kline(ret_concept_kline_min,
                                                freq=QA.FREQUENCE.FIFTEEN_MIN)

                    ret_concept_kline_day = fetch_stock_concept_from_eastmoney(stock_bk=code,
                                                                               freq=QA.FREQUENCE.DAY, )

                    if (ret_concept_kline_day is None):
                        continue

                    GQ_save_stock_concept_kline(ret_concept_kline_day,
                                                freq=QA.FREQUENCE.DAY)
                except Exception as e:
                    traceback.print_exception(type(e), e, sys.exc_info()[2])
                    print(u'Failed:{}\n'.format(code), e)
                time.sleep(delay_gap)
        except KeyboardInterrupt:
            overall_progress.close()
            raise
        return
    else:
        # todo 检查股票代码是否合法
        # return
        #
        pass
        print(u'代码未完成')
        stock_concept_components_df = crawl_stock_concept_components(code, code_metadata)[[AKA.CLOSE,
                                                                                           FLD.PCT_CHANGE,
                                                                                           'delta',
                                                                                           AKA.CODE,
                                                                                           AKA.NAME,
                                                                                           'concept_symbol',
                                                                                           'concept_name',
                                                                                           FLD.SOURCE]]
        data = stock_concept_components_df
        save_stock_concept_components(code, data, )
        return

    return stock_concept_component_df

def crawl_stock_concept_components(concept: str = 'BK05666', stock_concept_block: pd.DataFrame = None):
    '''
    抓取概念板块成分
    '''
    ret_stock_concept_components = GQ_fetch_stock_concept_components(concept=concept)
    ret_stock_concept_components[u'概念'] = stock_concept_block[AKA.NAME].item()
    ret_stock_concept_components[AKA.NAME] = stock_concept_block[AKA.NAME].item()
    last_tradedate = QA_util_get_pre_trade_date('{}'.format(datetime.date.today()), n=1)
    ret_stock_concept_components['date'] = last_tradedate
    ret_stock_concept_components['date'] = pd.to_datetime(ret_stock_concept_components['date'], )
    ret_stock_concept_components['date'] = ret_stock_concept_components['date'].dt.strftime('%Y-%m-%d')

    # GMT+0 String 转换为 UTC Timestamp
    ret_stock_concept_components["revision"] = pd.to_datetime(ret_stock_concept_components['date']).astype(
        np.int64) // 10 ** 9
    ret_stock_concept_components["date_stamp"] = pd.to_datetime(ret_stock_concept_components['date']).astype(
        np.int64) // 10 ** 9
    return ret_stock_concept_components



def save_stock_concept_components(concept: str = None,
                                  data: pd.DataFrame = None):
    '''
    保存概念板块成分，保存概念板块行情。
    '''
    # 查询是否新数据
    GQ_save_stock_concept_components(data, )
    return
def fetch_stock_concept_from_eastmoney(stock_bk: str = "BK05666",
                                       freq=QA.FREQUENCE.HOUR, ) -> pd.DataFrame:
    """
    东方财富网 > 行情中心 > 沪深板块
    http://quote.eastmoney.com/center/hsbk.html
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    http://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112404939798105940868_1629173273789&secid=90.BK0917&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=60&fqt=0&beg=19900101&end=20220101&_=1629173274401
    """
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fqt": "0",
        "klt": "101" if freq == QA.FREQUENCE.DAY else ("15" if freq == QA.FREQUENCE.FIFTEEN_MIN else "60"),
        "secid": f"90.{stock_bk}",
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "cb": "jQuery1124005797095004732822_{:d}".format(int(dt.utcnow().timestamp())),
        "_": int(time.time() * 1000),
        'beg': '19900101',
        'end': '20250101',
    }
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    json_data = json.loads(text_data[text_data.find("{"): -2])
    # print(stock_bk, json_data)
    if (isinstance(json_data, dict)):
        if (json_data["data"] is None):
            return None
        try:
            content_list = json_data["data"]["klines"]
        except Exception as e:
            print(type(json_data))
            print(json_data)
            traceback.print_exception(type(e), e, sys.exc_info()[2])
            print(u'Failed:{}\n'.format(stock_bk), e)
            return None
        # print(json_data)

        temp_df = pd.DataFrame([item.split(",") for item in content_list])
        # print(temp_df)
        temp_df.columns = [AKA.DATETIME,
                           AKA.OPEN,
                           AKA.CLOSE,
                           AKA.HIGH,
                           AKA.LOW,
                           AKA.VOLUME,
                           AKA.AMOUNT,
                           AKA.TURNOVER, ]
        # temp_df = temp_df.iloc[:, :-2]
        # print(temp_df)
        temp_df.loc[:, [AKA.OPEN,
                        AKA.CLOSE,
                        AKA.HIGH,
                        AKA.LOW,
                        AKA.VOLUME,
                        AKA.AMOUNT,
                        AKA.TURNOVER, ]] = temp_df[[AKA.OPEN,
                                                    AKA.CLOSE,
                                                    AKA.HIGH,
                                                    AKA.LOW,
                                                    AKA.VOLUME,
                                                    AKA.AMOUNT,
                                                    AKA.TURNOVER, ]].astype(np.float64)
        temp_df[AKA.SYMBOL] = stock_bk
        temp_df[AKA.TURNOVER] = temp_df[AKA.TURNOVER] / 100

        if (freq == QA.FREQUENCE.HOUR) or \
                (freq == QA.FREQUENCE.FIFTEEN_MIN):
            if (freq == QA.FREQUENCE.HOUR):
                temp_df['type'] = QA.FREQUENCE.HOUR
            if (freq == QA.FREQUENCE.FIFTEEN_MIN):
                temp_df['type'] = QA.FREQUENCE.FIFTEEN_MIN

            temp_df['date'] = pd.to_datetime(temp_df[AKA.DATETIME])
            temp_df['date'] = temp_df['date'].dt.strftime('%Y-%m-%d')
            temp_df['datetime'] = pd.to_datetime(temp_df[AKA.DATETIME], )
            temp_df['datetime'] = temp_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # GMT+0 String 转换为 UTC Timestamp
            temp_df['time_stamp'] = pd.to_datetime(temp_df['datetime']).astype(np.int64) // 10 ** 9
        elif (freq == QA.FREQUENCE.DAY):
            temp_df['date'] = pd.to_datetime(temp_df[AKA.DATETIME])
            temp_df['date'] = temp_df['date'].dt.strftime('%Y-%m-%d')

        temp_df["date_stamp"] = pd.to_datetime(temp_df['date']).astype(np.int64) // 10 ** 9

        # print(temp_df)
        return temp_df
    else:
        return None


def get_stock_concept_list(
        code=None,
        symbol=None,
        format='pd',
        collections=DATABASE.stock_concept_list,
):
    """
    获取概念板块列表
    """
    if code is not None:
        symbol = code
    if symbol is not None:
        symbol = QA_util_code_tolist(symbol)
        data = pd.DataFrame(
            [
                item for item in
                collections.find({AKA.SYMBOL: {
                    '$in': symbol
                }, 'revision': {
                    '$gt': QA_util_date_stamp(dt.now() - timedelta(days=10))
                }},
                    batch_size=10000)
            ]
        ).drop(['_id'],
               axis=1)
        return data.set_index(AKA.SYMBOL, drop=False).drop_duplicates(AKA.SYMBOL, 'last')
    else:
        data = pd.DataFrame([item for item in collections.find()]
                            ).drop(['_id'],
                                   axis=1)
        return data.set_index(AKA.SYMBOL, drop=False).drop_duplicates(AKA.SYMBOL, 'last')


def get_stock_concepts(symbol=None,
                       format='pd',
                       collections=DATABASE.stock_concept_components):
    """
    获取个股涉及的概念板列表
    """
    if symbol is not None:
        symbol = QA_util_code_tolist(symbol)
        data = pd.DataFrame(
            [
                item for item in
                collections.find({AKA.CODE: {
                    '$in': symbol
                }, 'revision': {
                    '$gt': QA_util_date_stamp(dt.now() - timedelta(days=10))
                }},
                    batch_size=10000)
            ]
        ).drop(['_id'],
               axis=1)
        return data.set_index('concept_symbol', drop=False).drop_duplicates(['concept_symbol', AKA.CODE], 'last')
    else:
        data = pd.DataFrame([item for item in collections.find()]
                            ).drop(['_id'],
                                   axis=1)
        return data.set_index('concept_symbol', drop=False).drop_duplicates(['concept_symbol', AKA.CODE], 'last')

    return


def get_stock_concept_components(symbol=None,
                                 format='pd',
                                 collections=DATABASE.stock_concept_components):
    """
    获取概念板块成分股列表
    """
    if symbol is not None:
        symbol = QA_util_code_tolist(symbol)
        data = pd.DataFrame(
            [
                item for item in
                collections.find({'concept_symbol': {
                    '$in': symbol
                }, 'revision': {
                    '$gt': QA_util_date_stamp(dt.now() - timedelta(days=10))
                }},
                    batch_size=10000)
            ]
        ).drop(['_id'],
               axis=1)
        return data.set_index('concept_symbol', drop=False).drop_duplicates(['concept_symbol', AKA.CODE], 'last')
    else:
        data = pd.DataFrame([item for item in collections.find()]
                            ).drop(['_id'],
                                   axis=1)
        return data.set_index('concept_symbol', drop=False).drop_duplicates(['concept_symbol', AKA.CODE], 'last')

    return


def get_stock_concept_kline(symbol: str = None,
                            freq=QA.FREQUENCE.DAY,
                            start: str = None,
                            end: str = None,
                            format: str = 'pd',
                            collections=DATABASE.stock_concept_day) -> pd.DataFrame:
    """'获取股票资金流向'

    Returns:
        [type] -- [description]
    """
    if (freq != QA.FREQUENCE.DAY):
        collections = DATABASE.stock_concept_min

    start = '{}'.format(datetime.date.today() - timedelta(days=2500)) if (start is None) else str(start)[0:10]
    end = '{}'.format(datetime.date.today() + timedelta(days=1)) if (end is None) else str(end)[0:10]
    # code= [code] if isinstance(code,str) else code

    # code checking
    symbol = QA_util_code_tolist(symbol)
    if QA_util_date_valid(end):
        cursor = collections.find({
            AKA.SYMBOL: {
                '$in': symbol
            },
            "date_stamp":
                {
                    "$lte": QA_util_date_stamp(end),
                    "$gte": QA_util_date_stamp(start)
                }
        },
            {"_id": 0},
            batch_size=10000)
        # res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            res = res.assign(date=pd.to_datetime(res.date)).drop_duplicates((['date',
                                                                              'symbol'])).set_index(['date',
                                                                                                     'symbol'],
                                                                                                    drop=False)
        except Exception as e:
            print(u'get_stock_concept_kline:{}'.format(symbol), e)
            print(res.columns)
            pass

        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print(
                "QA Error get_stock_concept_kline format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info('QA Error get_stock_concept_kline data parameter start=%s end=%s is not right' % (start,
                                                                                                           end))
    return res



if __name__ == '__main__':
    GQ_SU_crawl_stock_concept_components_from_eastmoney()