from stockquant.quant import *


config.loads('config.json')

count = 0
while True:
    if count < 3:
        tick = Market.tick("sh600519")
        DingTalk.text("股票交易实时价格提醒：股票名称：{} 当前价格：{}".format(tick.symbol, tick.last))
        count += 1