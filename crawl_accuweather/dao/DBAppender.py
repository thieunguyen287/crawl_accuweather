from datetime import datetime

import numpy as np
import mysql
from mysql.connector import cursor

import sys


class DBAppender(object):
    def __init__(self, table_name='test_table', fields=('time', 'value'), *args, **kwargs):
        self.table_name = table_name
        self.fields = fields or []
        self.config = {
            'user': 'scrapy',
            'password': 'Y0h0h0h0@@',
            'host': '128.199.223.164',
            'database': 'nep_demand',
            'raise_on_warnings': True,
        }

    def zip_insert(self, *data_lists):
        """
        Insert or replace data
        :param data_lists: lists of fields' values corresponding to self.fields
        :return:
        """
        try:
            cnx = mysql.connector.connect(**self.config)
            cs = cnx.cursor()
            sql = "replace into " + self.table_name + " " + \
                  "(" + ",".join(self.fields) + ") " + \
                  "values(" + ",".join(len(self.fields) * ["%s"]) + ")"
            # print sql
            cs.executemany(sql, zip(*data_lists))
            cnx.commit()
            cs.close()
            cnx.close()
            print 'Data inserted'
        except:
            print "Unexpected error:", sys.exc_info()

    def insert(self, data):
        """
        Insert or replace data
        :param data: data tuples to insert
        :return:
        """
        try:
            cnx = mysql.connector.connect(**self.config)
            cs = cnx.cursor()
            sql = "replace into " + self.table_name + " " + \
                  "(" + ",".join(self.fields) + ") " + \
                  "values(" + ",".join(len(self.fields) * ["%s"]) + ")"
            # print sql
            cs.executemany(sql, data)
            cnx.commit()
            cs.close()
            cnx.close()
            print 'Data inserted'
        except:
            print "Unexpected error:", sys.exc_info()

if __name__ == '__main__':
    appender = DBAppender()
    dt1 = datetime(1990, 1, 2, 3)
    dt2 = datetime(2000, 4, 5, 6)
    for i in range(10):
        appender.zip_insert([dt1, dt2], [float(-i), float(i)])
