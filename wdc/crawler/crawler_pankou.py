import pymongo
from wdc.util.WdcMarket import WdcMarket
import QUANTAXIS as QA
import pandas as pd
from pymongo import UpdateOne

from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_code_tolist,
    QA_util_date_stamp,
    QA_util_date_valid,
    QA_util_log_info,
    QA_util_to_json_from_pandas
)

wdcMarket = WdcMarket()
def fetch_pankou_from_baostock(code=None,start_date=None,end_date=None):
    data = wdcMarket.get_pankou(code,start_date=start_date,end_date=end_date)
    #print(result)
    coll = DATABASE.stock_pankou_day
    # 初始化更新请求列表
    update_requests = []

    indexes = set(data.index)
    for index in indexes:
        doc = dict(data.loc[index])
        try:
            update_requests.append(UpdateOne({'code': doc['code'],'date':doc['date']}, {'$set': doc}, upsert=True))

        except Exception as e:
            print('Error:', e)

        # 如果抓到了数据
    if len(update_requests) > 0:
        update_result = coll.bulk_write(update_requests, ordered=False)

        print('code:%s,盘口更新， 插入：%4d条，更新：%4d条' %
              (code,update_result.upserted_count, update_result.modified_count), flush=True)

def compute_shizhi(code,start_date=None,end_date=None):
    pd_stock = QA.QA_fetch_stock_day(code,start_date,end_date,format='p')
    pd_stock = pd_stock.set_index(['date','code'],drop=False)
    pd_finance = QA.QA_fetch_financial_report_adv(code,start_date,end_date).data
    pd_finance = pd_finance.rename(columns={'report_date': 'date'})
    pd_finance = pd_finance.set_index(['date','code'],drop=False)
    pd_shizhi = pd.merge(pd_stock['close'], pd_finance['totalCapital'], left_index=True,right_index=True,how='outer')
    pd_shizhi = pd_shizhi.reset_index()
    pd_shizhi.fillna(method='ffill',inplace=True)
    pd_shizhi['shiZhi'] = pd_shizhi['close']*pd_shizhi['totalCapital']
    pd_shizhi.drop(['close', 'totalCapital'], axis=1, inplace=True)
    pd_shizhi = pd_shizhi.dropna()
    #print(pd_shizhi)

    coll = DATABASE.stock_pankou_day
    # 初始化更新请求列表
    update_requests = []

    indexes = set(pd_shizhi.index)
    for index in indexes:
        doc = dict(pd_shizhi.loc[index])
        try:
            update_requests.append(UpdateOne({'code': doc['code'], 'date': doc['date']}, {'$set': doc}, upsert=True))

        except Exception as e:
            print('Error:', e)

        # 如果抓到了数据
    if len(update_requests) > 0:
        update_result = coll.bulk_write(update_requests, ordered=False)

        print('code:%s,市值更新， 插入：%4d条，更新：%4d条' %
              (code, update_result.upserted_count, update_result.modified_count), flush=True)

def fetch_stock_pankou(code):
    try:
        compute_shizhi(code, start_date='2021-01-01', end_date='2021-12-31')
        #fetch_pankou_from_baostock(code, start_date='1991-01-01', end_date='2021-12-31')
    except Exception as e:
        print("error code:",code,e)
    wdcMarket.logout()

if __name__ == '__main__':
    #compute_shizhi('000001', start_date='2007-12-31', end_date='2009-01-01')
    #fetch_pankou_from_baostock('000001', start_date='2007-12-31', end_date='2009-01-01')

    df_stocks = QA.QA_fetch_stock_list()
    #codes = set(df_stocks.index)
    codes = ['002686']
    for code in codes:
        fetch_stock_pankou(code)

