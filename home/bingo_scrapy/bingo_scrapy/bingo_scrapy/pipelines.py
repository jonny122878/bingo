import csv
import sqlite3
from collections import defaultdict
import logging
import pymssql
from dotenv import load_dotenv
import os
import random
class CsvPipeline:
    def open_spider(self, spider):
        self.file = open('bingo_results.csv', 'w', newline='')
        self.writer = csv.writer(self.file)
        
        self.writer.writerow(['date', 'drawTerm', 'dDate', 'bigShowOrder'])

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):

        date = item['date']
        items = item['items']
        for i in items:
            self.writer.writerow([date, i['drawTerm'], i['dDate'], ",".join(i['bigShowOrder'])])
        return item


load_dotenv()
server = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
port = os.getenv('DB_PORT')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')
test_pattern = os.getenv('TEST_PATTERN')
print(f"DB_HOST: {server}")
print(f"DB_PORT: {port}")
print(f"DB_USER: {user}")
print(f"DB_PASSWORD: {password}")
print(f"DB_DATABASE: {database}")

class DbPipeline:
    def __init__(self):
        self.counter = 0
    def open_spider(self, spider):
        
        logging.info("Opening spider and establishing database connection.")
        try:
            self.conn = pymssql.connect(server=server, user=user, password=password, database=database)
            self.cursor = self.conn.cursor()
        except pymssql.Error as e:
            logging.error(f"Error connecting to database: {e}")
        self.schema = spider.settings.get('DB_SCHEMA')
        self.table = spider.settings.get('DB_TABLE')
        
        #取得設定檔中的DEBUG值，如果是True，則刪除資料表
        logging.info(f"DELETE_OLD_DATA: {spider.settings.get('DELETE_OLD_DATA')}")
        if spider.settings.getbool('DELETE_OLD_DATA'):
            self.cursor.execute(f'DROP TABLE IF EXISTS {self.schema}.{self.table}')
            self.conn.commit()
            logging.info("Table dropped.")
            print("Table dropped.")
        logging.info(f"Checking if table {self.schema}.{self.table} exists.")
        self.cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}'")
        if self.cursor.fetchone()[0] == 0:
            logging.info(f"Table {self.schema}.{self.table} does not exist. Creating table.")
            self.cursor.execute(f'''
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{self.table}' AND xtype='U')
                CREATE TABLE {self.schema}.{self.table} (
                    drawTerm VARCHAR(255),
                    dDate DATE,
                    bigShowOrder VARCHAR(255),
                    createDate DATETIME DEFAULT GETDATE()
                )
            ''')
            logging.info(f"Table {self.schema}.{self.table} created successfully.")
        else:
            logging.info(f"Table {self.schema}.{self.table} already exists.")
        self.conn.commit()
        logging.info("Database connection established and table checked/created.")
    
    def close_spider(self, spider):
        self.conn.close()
        logging.info("Database connection closed.")
    def test_error(self, sub_item, spider):
        self.counter+=1
        test_pattern = os.getenv('TEST_PATTERN')
        #print(f"counter:{self.counter}")
        if test_pattern == "random":
            if spider.settings.get('DEBUG') and 1==random.randint(0,500):
                #print產生錯誤的資料日期
                print(f"Error in processing item: {sub_item['dDate']}")
                logging.error(f"Error in processing item: {sub_item['dDate']}")
                raise Exception("Error in processing item.")
        
        elif test_pattern == "count":
            if self.counter ==110:
                #print產生錯誤的資料日期
                print(f"Error in processing item: {sub_item['dDate']+str(self.counter)}")
                logging.error(f"Error in processing item: {sub_item['dDate']},"+str(self.counter))
                raise Exception("Error in processing item.")
    def process_item(self, item, spider):
        try:
            conn = pymssql.connect(server=server, user=user, password=password, database=database)
            cursor = conn.cursor()
            ##先刪除欲新增日期的舊資料
            cursor.execute(f'''
                DELETE FROM {self.schema}.{self.table} WHERE dDate LIKE %s
            ''', (item['date'] + '%',))
            for sub_item in item['items']:
 #在DEBUG模式中，5000分之一的概率產生錯誤
                if spider.settings.getbool('DEBUG'):
                    self.test_error(sub_item,spider)
                cursor.execute(f'''
                    INSERT INTO {self.schema}.{self.table} (drawTerm, dDate, bigShowOrder, createDate) VALUES (%s, %s, %s, GETDATE())
                ''', (sub_item['drawTerm'], sub_item['dDate'], ','.join(sub_item['bigShowOrder'])))
            conn.commit()
            logging.info("Item processed and committed to database.")
        except pymssql.Error as e:
            logging.error(f"Error processing item: {e}")
            conn.rollback()
        finally:
            conn.close()
            logging.info("Database connection closed after processing item.")
        return item
