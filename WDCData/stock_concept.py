import numpy as np
import pandas as pd
import QUANTAXIS as QA
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_code_tolist,
    QA_util_date_stamp,
    QA_util_date_valid,
    QA_util_log_info,
    QA_util_to_json_from_pandas
)

from datetime import (
    datetime as dt,
     timedelta
)
import datetime
import pymongo
from WDCUtil.common import (AKA,FLD)


def init_index():

    coll = DATABASE.stock_concept_list
    coll.create_index([(AKA.SYMBOL, pymongo.ASCENDING)], unique=True)

    coll = DATABASE.stock_concept_day
    coll.create_index([(AKA.SYMBOL,pymongo.ASCENDING),("date",pymongo.ASCENDING)], unique=True)

    coll = DATABASE.stock_concept_min
    coll.create_index([(AKA.SYMBOL,pymongo.ASCENDING),
                       ("type",pymongo.ASCENDING),
                       (FLD.DATETIME,pymongo.ASCENDING) ],
                      unique=True)

    coll = DATABASE.stock_concept_components
    coll.create_index([('concept_symbol',pymongo.ASCENDING),
                       ("date",pymongo.ASCENDING)],
                      unique=False)
    coll.create_index([('concept_name',pymongo.ASCENDING),
                       ("date",pymongo.ASCENDING)])
    coll.create_index([(AKA.CODE,pymongo.ASCENDING),("concept_symbol",pymongo.ASCENDING),("date",pymongo.ASCENDING)],
                      unique=True)

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
            return np.asarray(res)
        elif format in ['list', 'l', 'L']:
            return np.asarray(res).tolist()
        else:
            print(
                "QA Error get_stock_concept_kline format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info('QA Error get_stock_concept_kline data parameter start=%s end=%s is not right' % (start,
                                                                                                           end))
    return res

if __name__ == "__main__":
    init_index()
