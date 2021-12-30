from stockquant.quant import *                                                              # 导入模块


class Strategy:                                                                             # 定义策略类

    def __init__(self):
        config.loads('config.json')                                                         # 载入配置文件
        self.asset = 10000                                                                  # 总资金
        self.backtest = BackTest()                                                          # 初始化回测模块
        data = Market.kline('sh600519', '1d')                                               # 获取k线数据
        self.kline = []                                                                     # 创建空的列表盛放递增的k线数据
        for k in data:                                                                      # 遍历历史k线数据
            self.kline.append(k)                                                            # 将k线依次追加至空列表中
            self.backtest.initialize(self.kline, data)                                      # k线入口函数，传入递增k线与原始k线
            self.begin()                                                                    # 调用begin方法
        plot_asset()                                                                        # 回测完成绘制资金曲线图

    def begin(self):                                                                        # 定义实例方法，策略主体

        if CurrentBar(self.kline) < 30:                                                     # 如果k线长度小于计算指标需要的k线长度
            return                                                                          # 返回

        ma20 = MA(20, self.kline)                                                           # 计算ma20
        ma30 = MA(30, self.kline)                                                           # 计算ma30
        cross_over = ma20[-1] > ma30[-1] and ma20[-2] <= ma30[-2]                           # 计算金叉
        cross_down = ma20[-1] < ma30[-1] and ma20[-2] >= ma30[-2]                           # 计算死叉

        if cross_over and self.backtest.long_quantity == 0:                                 # 如果金叉且当前未持股，就买入股票
            self.backtest.buy(                                                              # 模拟买入方法
                price=self.backtest.close,                                                  # 价格为此根k线收盘价
                amount=self.asset/self.backtest.close,                                      # 数量为总资金除以买入价格
                long_quantity=self.asset/self.backtest.close,                               # 当前持多数量
                long_avg_price=self.backtest.close,                                         # 当前持多价格
                profit=0,                                                                   # 利润
                asset=self.asset                                                            # 总资金
            )

        elif cross_down and self.backtest.long_quantity != 0:                               # 如果死叉且当前持股，就卖出股票
            profit = (self.backtest.close - self.backtest.long_avg_price) * self.backtest.long_quantity     # 计算利润
            self.asset += profit                                                            # 计算变化后的总资金
            self.backtest.sell(                                                             # 模拟卖出方法
                price=self.backtest.close,                                                  # 卖出价格为当根k线收盘价
                amount=self.backtest.long_quantity,                                         # 卖出数量为当前持股数量
                long_quantity=0,                                                            # 卖出后当前持股数量为0
                long_avg_price=0,                                                           # 卖出后当前持股价格为0
                profit=profit,                                                              # 利润
                asset=self.asset                                                            # 总资金
            )

        # 如果当前有持股，且最低价小于等于止损价格，就卖出股票止损
        if self.backtest.long_quantity > 0 and self.backtest.low <= self.backtest.long_avg_price * 0.9:
            profit = (self.backtest.low - self.backtest.long_avg_price) * self.backtest.long_quantity
            self.asset += profit
            self.backtest.sell(
                price=self.backtest.long_avg_price * 0.9,
                amount=self.backtest.long_quantity,
                long_quantity=0,
                long_avg_price=0,
                profit=profit,
                asset=self.asset
            )


if __name__ == '__main__':

    strategy = Strategy()