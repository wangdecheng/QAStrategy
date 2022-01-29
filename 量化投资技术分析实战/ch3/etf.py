import QUANTAXIS as QA
import WDCData

def get_shizhi_and_pe(block_name,date):
    try:
        df_block = QA.QA_fetch_stock_block_adv(blockname=block_name).data
        df_block = df_block[df_block['type'] == 'zs']  # zs:指数
        codes = [code[1] for code in df_block.index.to_list()]
    except:
        df_block = WDCData.StockConceptComponents.query_components(block_name)
        codes = df_block['code'].to_list()


    df_fund = WDCData.StockPankouDay.query_fundamentals(codes, date)
    return (df_fund['shiZhi'].mean(),df_fund['peTTM'].astype(float).mean())

def index_history_show():
    start_date = '2010-01-01'
    end_date = '2018-03-07'
    df_50 = QA.QA_fetch_index_day('000016',start=start_date,end=end_date,format='p')['close']
    print(df_50)

if __name__ == '__main__':
    #df_50 = get_shizhi_and_pe(block_name='上证50', date='2018-03-07')
    #df_300 = get_shizhi_and_pe(block_name='沪深300', date='2018-03-07')
    #df_500 = get_shizhi_and_pe(block_name='中证500', date='2018-03-07')
    index_history_show()



