from stockquant.quant import *


config.loads('config.json')
kline = Market.kline("sh600519", "5m")
print(kline)
result = Market.shanghai_component_index()		# 获取上证综指
# result = Market.shenzhen_component_index()		# 获取深圳成指
# result = Market.stocks_list()		# 股票列表,获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
# result = Market.today_is_open()	# 查询今日沪深股市是否开盘，如果开盘返回True,反之False
# result = Market.new_stock()		# 获取新股上市列表数据
print(result)