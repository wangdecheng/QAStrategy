import pandas as pd
from QUANTAXIS.QAUtil import (
    DATABASE
)
import pymongo

_table = DATABASE.stock_fundamentals

def init_index():

    _table.create_index([('code',pymongo.ASCENDING),("date",pymongo.ASCENDING)], unique=True)

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
   init_index()