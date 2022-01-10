import pymongo
from wdc.util.WdcMarket import WdcMarket
from WDCUtil import logger
import QUANTAXIS as QA
import pandas as pd
from pymongo import UpdateOne
from WDCUtil import WDCMongo
import traceback
import sys

import WDCData as wdcData

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
    if wdcData.StockPankouDay.exists(code):
        return
    data = wdcMarket.get_pankou(code,start_date=start_date,end_date=end_date)
    if data is None:
        logger.error("not found pan_kou data of code:", code)
    #print(result)
    coll = DATABASE.stock_pankou_day
    print(code, '开始更新盘口===')
    WDCMongo.save_df(data,coll,['code','date'])


def compute_shizhi(code,start_date=None,end_date=None):
    if wdcData.StockPankouDay.exists_shizhi(code):
        return
    pd_stock = QA.QA_fetch_stock_day(code,start_date,end_date,format='p')
    if(pd_stock is None):
        logger.error("not found k_day of code:"+code)
        return
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
    print(code,'开始更新市值===')
    WDCMongo.save_df(pd_shizhi,coll,['code','date'])


def fetch_stock_pankou(code):
    try:
        compute_shizhi(code, start_date='2007-12-31', end_date='2021-12-31')
        fetch_pankou_from_baostock(code, start_date='1991-01-01', end_date='2021-12-31')
    except Exception as e:
        traceback.print_exception(type(e), e, sys.exc_info()[2])
        print("error code:", code, e)


if __name__ == '__main__':
    #compute_shizhi('000001', start_date='2007-12-31', end_date='2009-01-01')
    #fetch_pankou_from_baostock('000001', start_date='2007-12-31', end_date='2009-01-01')

    df_stocks = QA.QA_fetch_stock_list()
    codes = set(df_stocks.index)
    #codes = ['300522']
    print('股票总数：',len(codes))
    count = 0
    for code in codes:
        fetch_stock_pankou(code)
        count = count +1
    wdcMarket.logout()
    print('更新总数：',count)