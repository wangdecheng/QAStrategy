from stockquant.quant import *


config.loads('config.json')

tick = Market.tick("sh601003")
last = tick.last
print(tick.symbol, tick.last, tick.timestamp)

if last < 100:
    DingTalk.text(
        "交易：价格已小于100元！\n"
        "当前价格：{}".format(last)
    )
    sendmail(
        "交易：价格已大于100元！\n"
        "当前价格：{}".format(last)
    )