# -*- coding:utf-8 -*-
from abc import ABC, abstractmethod
from datetime import date, datetime
import pyodbc


class IDbContext(ABC):

    @property
    def err(self):
        return self.__err

    @err.setter
    def err(self, value):
        self.__err = value

    def __init__(self):
        self.__err = ''

    @abstractmethod
    def getCollection(self, DBCollection):
        pass

    @abstractmethod
    def Insert(self, DBCollection, DBData):
        pass

    @abstractmethod
    def Find(self, DBCollection, DBQuery):
        pass

    @abstractmethod
    def Delete(self, DBCollection, DBQuery):
        pass


class MSSQLDbContext:
    def __init__(self, db_setting):
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_setting['server']};"
            f"DATABASE={db_setting['database']};"
            f"UID={db_setting['user']};"
            f"PWD={db_setting['password']}"
        )
        self.conn = pyodbc.connect(conn_str)
        self.cur = self.conn.cursor()

    def select(self, sql):
        self.cur.execute(sql)
        columns = [column[0] for column in self.cur.description]
        rows = [dict(zip(columns, row)) for row in self.cur.fetchall()]
        return rows

    def delete(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
            return ''
        except Exception as e:
            return e

    def insert(self, table, datas):
        sql = ''
        for data in datas:
            sql += self.getInsertSQL(table, data)

        try:
            self.cur.execute(sql)
            self.conn.commit()
            return ''
        except Exception as e:
            return e

    def getInsertSQL(self, table, data):
        tableKey = '('
        tableValue = '('
        for key, value in data.items():
            tableKey += key+','

            if value == "CURRENT_TIMESTAMP":
                tableValue += value+","
            elif type(value) == int:
                tableValue += str(value)+","
            elif type(value) == datetime or type(value) == date:
                tableValue += "'"+str(value)+"',"
            else:
                tableValue += "'"+value+"',"

        tableKey += ')'
        tableValue += ')'
        tableKey = tableKey.replace(",)", ")")
        tableValue = tableValue.replace(",)", ")")
        sql = "insert into "+table+tableKey+" VALUES"+tableValue
        return sql

    def close(self):
        self.cur.close()
        self.conn.close()


# 原始資料
# |drawTerm|bigShowOrder|
# 表頭
# |drawTerm|algorithmName|amt|createdTime|
# 表身
# |drawTerm|nums|matches|double|amt|createdTime|
