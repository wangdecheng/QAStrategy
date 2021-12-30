from stockquant.quant import *

class Stragety:
    def __init__(self):
        config.loads(config_file="config.json")
        self.amount,self.price = 0,0

    def begin(self):
        if get_localtime()[11:16] == "16:00" and Market.today_is_open():
            kline = Market.kline('sh600519','1d')
            ma20 = MA(20,kline)