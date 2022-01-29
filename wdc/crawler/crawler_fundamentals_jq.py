from jqdatasdk import *
import QUANTAXIS as QA
from WDCUtil import WDCMongo
import numpy as np
import time

import WDCData as wdcData

from QUANTAXIS.QAUtil import (
    DATABASE
)

# https://www.joinquant.com/help/api/help#api:API%E6%96%87%E6%A1%A3
# https://www.joinquant.com/help/api/help#JQData:JQData

# aa 为你自己的帐号， bb 为你自己的密码
auth('17600199926','199926')


def fetch_stock_fundametals(day):
    #df = get_fundamentals(query(valuation).filter(valuation.code.in_(['000001.XSHE', '600000.XSHG'])), date=day)
    df = get_fundamentals(query(valuation), date=day)
    df['code'] = [x[:6] for x in df['code']]
    df['date'] = df['day']
    df.drop(columns=['day','id'],inplace=True)
    #print(df)
    coll = DATABASE.stock_fundamentals
    print(day,'开始更新财务数据===')
    WDCMongo.save_df(df, coll, ['code', 'date'])

if __name__ == '__main__':
    nd_days = get_trade_days(start_date="2007-12-07", end_date="2010-12-31")
    for day in nd_days:
        fetch_stock_fundametals(day)
        time.sleep(0.8)
