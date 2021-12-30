from stockquant.quant import *      # 导入模块


config.loads('config.json')         # 载入配置文件

while True:
    if get_localtime()[11: 16] == "16:00" and Market.today_is_open():

        stocks_pool = []                    # 空的股票池，将筛选出来的股票加入这个股票池中

        stocks_list = Market.stocks_list()  # 获取沪深市场所有股票的基础信息

        start_time = get_localtime()    # 获取本地时间
        print("筛选股票开始时间：", start_time)

        for item in stocks_list['code']:        # 遍历所有股票代码
            name = str(item).replace(".", "")  # 将股票的代码处理成我们需要的格式

            kline = Market.kline(name, "1d")    # 获取k线

            if len(kline) < 30:             # 容错处理，因为有些新股可能k线数据太短无法计算指标
                continue

            ma20 = MA(20, kline)        # 计算ma20和ma30
            ma30 = MA(30, kline)

            # 双均线交叉
            if ma20[-1] > ma30[-1] and ma20[-2] <= ma30[-2]:
                stocks_pool.append(name)

        print(stocks_pool)
        DingTalk.text("交易提醒之股票池：满足条件的股票有：{}".format(stocks_pool))
        end_time = get_localtime()    # 获取本地时间
        print("筛选股票开始时间：", end_time)
        sleep(1440*60)
    else:
        sleep(60*60)