from datetime import time

import QUANTAXIS as QA
import pandas as pd


def strategy001(data, N=40, mu=1):
    MP = QA.MA((data.high + data.low + data.close) / 3, N)
    concat = pd.concat(
        [abs(data.high - data.low), abs(data.high - data.close.shift(1)), abs(data.low - data.close.shift(1))], axis=1)
    TR = concat.max(axis=1)
    upBand = MP + mu * QA.MA(TR, N)
    dnBand = MP - mu * QA.MA(TR, N)
    FP = MP
    return pd.DataFrame({'MP': MP, 'TR': TR,'MPDiff':MP.diff(), 'upBand': upBand, 'dnBand': dnBand})

def change_key(idx):
    new_idx = idx[0].strftime("%Y-%m-%d")

    return (new_idx,idx[1])

if __name__ == "__main__":
    data = QA.QA_fetch_stock_day_adv('000009', '2019-01-01', '2019-05-01')

    ind = data.add_func(strategy001)
    xs = QA.QA_DataStruct_Indicators(ind)
    #print(xs.get_timerange("2019-02-01",'2019-03-01'))

   #QA.CROSS(ind.close,ind.upBand)//上穿upBand

    lastprice = 0
    user = QA.QA_User(username='quantaxis',password='quantaxis') #用户
    portfolio  = user.new_portfolio('strategy001')  #账户组合
    acc = portfolio.new_account(account_cookie='acc001',init_cash=100000)
    for idx, item in
        .iterrows():
        print(idx,item)
        #idx = change_key(idx)
        if MPDiff.loc[idx] > 0 and item['close'] > ind.upBand.loc[idx]:
            print('buyOpen _ {}'.format(idx))
            acc.receive_simpledeal(
                code=idx[1],
                trade_price=item['close'],
                trade_amount=1,
                trade_towards=QA.ORDER_DIRECTION.BUY_OPEN,
                trade_time=idx[0]
            )
        if MPDiff.loc[idx] < 0 and item['close'] < ind.dnBand.loc[idx]:
            print('sellOPEN_ {}'.format(idx))
            acc.receive_simpledeal(
                code=idx[1],
                trade_price=item['close'],
                trade_amount=1,
                trade_towards=QA.ORDER_DIRECTION.SELL_OPEN,
                trade_time=idx[0]
            )
        else:
            print('close')


        lastprice = item['close']
    print('end')

    print(acc.history_table.tail(10))

