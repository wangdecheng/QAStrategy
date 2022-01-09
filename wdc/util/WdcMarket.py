from wdc.util.baostockdata import BaoStockData
import baostock as bs
from wdc.util.logger import logger

class WdcMarket:

    def __init__(self):
        lg = bs.login()
        if lg.error_code != "0":
            logger.error("error: {}".format(lg.error_msg))
        logger.info("baostock init")
        self.bsd = BaoStockData()

    def logout(self):
        bs.logout()

    def get_pankou(self, code, start_date=None, end_date=None):
        """
                获取历史k线数据
                :param code:例如："sh601003"，或者"sz002307"，前者是沪市，后者是深市
                :param timeframe:k线周期，区分大小写，支持："5m", "15m", "30m", "1h", "1d", "1w", "1M，分别为5、15、30分钟、1小时、1天、1周、1月
                :param adj:复权类型，默认是"3"不复权；前复权:"2"；后复权:"1"。已支持分钟线、日线、周线、月线前后复权。 BaoStock提供的是涨跌幅复权算法复权因子
                :param start_date:开始日期（包含），格式“YYYY-MM-DD”，为空时取2015-01-01；
                :param end_date:结束日期（包含），格式“YYYY-MM-DD”，为空时取最近一个交易日；
                :return:返回一个列表,其中每一条k线数据包含在一个列表中
        """


        return self.bsd.query_history_k_data_plus(code, fields='date, code, peTTM, pbMRQ, psTTM, pcfNcfTTM, turn',
                                                  start_date=start_date, end_date=end_date)