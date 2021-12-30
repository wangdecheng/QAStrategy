from stockquant.quant import *


class Strategy:

    def __init__(self):
        config.loads('config.json')
        self.amount, self.price = 0, 0

    def begin(self):
        if get_localtime()[11: 16] == "16:00" and Market.today_is_open():
            kline = Market.kline('sh600519', '1d')
            ma20 = MA(20, kline)
            ma30 = MA(30, kline)
            cross_over = ma20[-1] > ma30[-1] and ma20[-2] <= ma30[-2]   # 金叉
            cross_down = ma20[-1] < ma30[-1] and ma20[-2] >= ma30[-2]   # 死叉
            if cross_over:
                DingTalk.text("交易提醒：sh600519 贵州茅台两根均线已经金叉，满足买入条件，请买入！")
                self.amount, self.price = 100, float(kline[-1][4])
            elif cross_down and self.amount == 0:   # 如果死叉且当前未持股
                DingTalk.text("交易提醒：sh600519 贵州茅台两根均线已经死叉，满足卖出条件，请卖出！")
                self.amount, self.price = 0, 0
        if self.amount > 0 and not not_open_time():     # 如果当前有持股
            tick = Market.tick('sh600519')
            last = tick.last        # 在交易时间段内获取一下当前的最新价格
            if last <= self.price * 0.9:
                DingTalk.text("交易提醒：sh600519 贵州茅台当前最新价格已达到止损位，请立即止损！")
                self.amount, self.price = 0, 0


if __name__ == '__main__':

    strategy = Strategy()

    while True:
        try:
            strategy.begin()
            sleep(3)
        except Exception as e:
            logger.error(e)