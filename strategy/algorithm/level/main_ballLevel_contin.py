from sqlite3 import connect
import pandas as pd
import numpy as np
import pymongo
# 計算hot 12、24、36、48、60整體維持rank,period用分割區間計算

if __name__ == '__main__':

    # region research continuous df
    value2Ds = [[True, True, False], [True, True, False], [True, True, True]]
    columns = ['1', '2', '3']
    df = pd.DataFrame(value2Ds, columns=columns)

    percentVals = []
    percenCols = []
    allTrueCount = 0
    for col in df.columns:
        df[f'{col}_shift'] = df[col].shift(1)
        df[f'{col}_calcu'] = df[f'{col}_shift'] == df[col]
        true_count = (df[f'{col}_calcu'] == True).value_counts()
        allTrueCount += true_count[True]
        percent = true_count / df[f'{col}_calcu'].count()
        rdPercent = round(percent[True], 2)
        percentVals.append(rdPercent)
        percenCols.append(col)
    dfPercent = pd.DataFrame([percentVals], columns=percenCols)
    print(df)
    print(dfPercent)
    print(allTrueCount)
    print(df.shape[1])
    allPercent = allTrueCount / df.shape[1]
    allPercent = round(allPercent, 2)
    print(allPercent)
    # endregion

    # region research df insert mongo
    connected = "mongodb://localhost:27017/"
    server = pymongo.MongoClient(connected)
    db = server['ballLevel']
    collection = db['ballLevelContin']
    queryKey = {"name": 'test'}
    result = collection.find(queryKey)
    for item in result:
        print(item)
    print(result)
    # dfPercent_dict = dfPercent.to_dict()
    # dicNum = {}
    # for key, value in dfPercent_dict.items():
    #     dicNum[key] = dfPercent_dict[key][0]
    # data = {"name": 'test', "nums": dicNum,
    #         "allPercent": allPercent}
    # collection.insert_one(data)
    # endregion

    # region db select 撈取資料
    # endregion

    # region divide 5 periods array
    # endregion

    # region 5 periods array to 5 ballLevelGroups
    # endregion

    # region convert to df row num , column period,value true or false
    # endregion

    # region df calcu ball continuous hot rank percent 2、3、4、5
    # endregion

    # region df calcu all ball continuous percent 2、3、4、5
    # endregion

    # region insert data to mongodb
    # endregion

    # copy cold and normal rank
    pass
