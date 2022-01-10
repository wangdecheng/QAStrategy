import time
import  pandas
def date_to_str(value):
    if type(value)== pandas._libs.tslibs.timestamps.Timestamp:
        return value.strftime('%Y-%m-%d')

    return value