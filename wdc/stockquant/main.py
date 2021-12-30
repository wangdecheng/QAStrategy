from stockquant.quant import *

config.loads("config.json")

tick = Market.tick(symbol="sh600519")
print(tick)

#kline = Market.kline("sh601003", "1d",start_date='2021-10-01')
#sendmail(str(kline))

#print(kline)

result = Market.shanghai_component_index()
print(result)

open = Market.today_is_open()
print(open)

print(Market.new_stock())