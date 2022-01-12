import pandas as pd
from QUANTAXIS.QAUtil import (
    DATABASE
)

_table = DATABASE.stock_pankou_day
date = '2021-11-30'  # 选最后一天，因为是批量插入，有值就证明存在
def exists(code, field='turn'):

    data = _table.find_one({'code':code,'date':date})
    if data is None:
        return False
    if data.get(field) is None:
        return False
    return True

def exists_shizhi(code):
    return exists(code,'shiZhi')

def query_fundamentals(codes,date):
    query_condition = {
        'date': date,
        'code': {
            '$in': codes
        }
    }
    item_cursor = _table.find(query_condition)
    items_from_collection = [item for item in item_cursor]
    df_data = pd.DataFrame(items_from_collection).drop(['_id'],axis=1)
    return df_data


if __name__ == "__main__":
    #print(exists_shizhi('300522'))
    #print(exists('300522'))
    print(query_fundamentals(['603501','603986'],'2018-01-10'))