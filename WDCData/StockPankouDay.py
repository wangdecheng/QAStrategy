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

if __name__ == "__main__":
    print(exists_shizhi('300522'))
    print(exists('300522'))