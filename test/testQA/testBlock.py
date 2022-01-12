import QUANTAXIS as QA
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams

if __name__ == '__main__':
    #QA.QAFetch.QATdx.QA_fetch_get_stock_block()
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        data = api.get_and_parse_block_info(TDXParams.BLOCK_SZ)
        print(api.to_df(data).to_csv("./tdx_zs.csv"))