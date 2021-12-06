import QUANTAXIS as QA
from QAStrategy.qastockbase import QAStrategyStockBase

class strategy(QAStrategyStockBase):

    def on_bar(self, data):
        # print(self.running_time)

        code = data.name[1]
        pos = self.get_positions(code)
        res = self.ma(code)
        if res.MA2[-1] > res.MA5[-1]:
            if pos.volume_long == 0:
                cash = self.get_cash()/2
                close = data['close']
                volume = int(cash/close/100)*100
                self.send_order('BUY', 'OPEN',code=code, price=close, volume=volume)
                print('Open', code)
        else:
            if pos.volume_long > 0:
                self.send_order('SELL', 'CLOSE', code=code, price=data['close'], volume=pos.volume_long)
                print('Close', code)


    def ma(self,code):
        self.code_market_data = self.get_code_marketdata(code)
        return QA.QA_indicator_MA(self.code_market_data, 2, 5)

if __name__ == '__main__':
    s = strategy(code=['000001', '000002'], frequence='day', start='2019-01-01', end='2020-04-01', strategy_id='stock_demo6')
    s.run_backtest()