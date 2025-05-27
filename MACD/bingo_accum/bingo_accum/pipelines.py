# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pdb
from unittest import signals
from itemadapter import ItemAdapter
import pandas as pd
import pymongo
from bingo_accum.db_nosql import MongoDbContext


class BingoAccumPipeline:
    def process_item(self, item, spider):
        # pdb.set_trace()
        return item


class MongoPipeline:
    def __init__(self):
        # 初始化 MongoDB 連接
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["bingo"]  # 指定資料庫名稱
        self.collection = self.db["bingo_accum_times"]  # 指定集合名稱

    def process_item(self, item, spider):
        # 將 item 轉換為字典並插入到 MongoDB
        # pdb.set_trace()
        self.collection.insert_one(ItemAdapter(item).asdict())
        return item

    def close_spider(self, spider):
        db = MongoDbContext("localhost", "bingo")
        table = "bingo_accum_times"
        queryKey = {}

        querys = db.Find(table, queryKey)
        results = list(querys)
        # 將 results 加載到 DataFrame
        df = pd.DataFrame(results)

        # 將 DataFrame 導出為 Excel 文件
        output_file = 'MACD_times.xlsx'
        df.to_excel(output_file, index=False, encoding='utf-8')
        # 關閉 MongoDB 連接
        self.client.close()
