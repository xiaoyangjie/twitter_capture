# -*- coding: UTF-8 -*-
"""
__author__ = 'YJ'

create_at :  2017/3/8

Function:
    实现将图片数据发往Hbase

History :
    first update time :2017./3/9

"""
import happybase

class HbaseInterface(object):

    def __init__(self, hbaseHost='192.168.178.212', hbasePort=9090, tableName='linuxtest'):
        self.hbaseHost = hbaseHost
        self.hbasePort = hbasePort
        self.tableName = tableName
        self.hbase = happybase.Connection(self.hbaseHost, self.hbasePort)
        self.hbaseTable = self.hbase.table(self.tableName)

    def sendPicture(self, key, data):
        self.hbaseTable.put(key, {'image:content': data})
