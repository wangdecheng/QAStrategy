import pandas as pd
from QUANTAXIS.QAUtil import (
    DATABASE
)

_table = DATABASE.stock_concept_components
date = '2021-12-31'  # 选最后一天，因为是批量插入，有值就证明存在


def query_components(concept_name):
    query_condition = {
        'date': date,
        'concept_name': concept_name
    }
    item_cursor = _table.find(query_condition)
    items_from_collection = [item for item in item_cursor]
    df_data = pd.DataFrame(items_from_collection).drop(['_id'],axis=1)
    return df_data


if __name__ == "__main__":

    print(query_components('HS300_'))