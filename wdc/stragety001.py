from datetime import time

import QUANTAXIS as QA
import pandas as pd


def strategy001(data, N=40, mu=1):
    MP = QA.MA((data.high + data.low + data.close) / 3, N)
    MA5 = QA.MA(data.close, 5)
    concat = pd.concat(
        [abs(data.high - data.low), abs(data.high - data.close.shift(1)), abs(data.low - data.close.shift(1))], axis=1)
    TR = concat.max(axis=1)
    upBand = MP + mu * QA.MA(TR, N)
    dnBand = MP - mu * QA.MA(TR, N)
    FP = MP
    return pd.DataFrame({'MP': MP, 'TR': TR,'MPDiff':MP.diff(), 'upBand': upBand, 'dnBand': dnBand,'MA5':MA5})

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
    portfolio = user.new_portfolio('strategy001')  #账户组合
    acc = portfolio.new_account(account_cookie='acc001',init_cash=1000000)
    MPDiff = ind.MPDiff
    MA5 = ind.MA5
    for idx, item in data.iterrows():
        #print(idx,item)
        #idx = change_key(idx)
        if item['close'] > MA5.loc[idx] and acc.hold_available.get(idx[1],0) ==0:
            print('buyOpen _ {}'.format(idx))
            acc.receive_simpledeal(
                code=idx[1],
                trade_price=item['close'],
                trade_amount=1,
                trade_towards=QA.ORDER_DIRECTION.BUY,
                trade_time=idx[0]
            )
        if item['close'] < MA5.loc[idx] and acc.hold_available.get(idx[1],0) ==0:
            print('sellOPEN_ {}'.format(idx))
            acc.receive_simpledeal(
                code=idx[1],
                trade_price=item['close'],
                trade_amount=1,
                trade_towards=QA.ORDER_DIRECTION.SELL,
                trade_time=idx[0]
            )
        else:
            print('no andy option {}'.format(idx))


        lastprice = item['close']
    print('end')

    print(acc.history_table.tail(10))
    performance = QA.QA_Performance(acc)
    print(performance.pnl_fifo)

