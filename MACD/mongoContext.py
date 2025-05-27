# 計算 option 手續費省掉錢

from bson import ObjectId
import pymongo
from datetime import datetime


class MongoDBHandler:
    def __init__(self, db_name, collection_name, uri="mongodb://localhost:27017/"):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def find_documents(self, query={}, projection=None):
        """
        查詢文檔並選擇特定字段
        :param query: 查詢條件
        :param projection: 投影（選擇特定字段）
        :return: 查詢結果列表
        """
        results = list(self.collection.find(query, projection))
        return results

    def insert_document(self, document):
        """
        插入新文檔
        :param document: 要插入的文檔
        :return: 插入結果
        """
        result = self.collection.insert_one(document)
        return result.inserted_id

    def update_document(self, query, update):
        """
        更新文檔
        :param query: 查詢條件
        :param update: 更新操作
        :return: 更新結果
        """
        result = self.collection.update_one(query, update)
        return result.modified_count

    def delete_document(self, query):
        """
        刪除文檔
        :param query: 查詢條件
        :return: 刪除結果
        """
        result = self.collection.delete_one(query)
        return result.deleted_count


if __name__ == '__main__':
    db_handler = MongoDBHandler(db_name='trade', collection_name='option')
    data = {}
    data['Name'] = '202503_21300_put'
    data['Qty'] = 2
    data['describe'] = '守紀律將衝動購入的選擇權賣出'
    data['profit'] = 200
    data['TradeDate'] = datetime(2025, 3, 13)
    db_handler.insert_document(data)
    pass
