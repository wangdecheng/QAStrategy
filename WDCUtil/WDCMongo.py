import pandas as pd
from pymongo import UpdateOne
from WDCUtil import WDCFormat


def save_df(df_data,db_table,key_fields):
    """
    key_fields:以什么字段更新
    """
    if 'datetime' in df_data.columns:
        df_data.datetime = df_data.datetime.apply(str)
    if 'date' in df_data.columns:
        df_data.date = df_data.date.apply(str).map(lambda x:x[:10])


    # 初始化更新请求列表
    update_requests = []

    indexes = set(df_data.index)
    for index in indexes:
        doc = dict(df_data.loc[index])
        try:
            key_dict = dict((k,doc[k]) for k in key_fields)
            update_requests.append(UpdateOne(key_dict, {'$set': doc}, upsert=True))

        except Exception as e:
            print('Error:', e)

        # 如果抓到了数据
    if len(update_requests) > 0:
        update_result = db_table.bulk_write(update_requests, ordered=False)

        print(' 数据：%4d条，插入：%4d条，更新：%4d条' %
              (len(update_requests), update_result.upserted_count, update_result.modified_count), flush=True)

