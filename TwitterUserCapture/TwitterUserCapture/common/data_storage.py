# -*- coding: UTF-8 -*-
# python2.7
# data_storage.py
# 数据库和kafka相关操作封装模块
from pymongo import MongoClient
import pymongo.errors
from TwitterUserCapture.common.constants import *
from TwitterUserCapture.interaction.kafka_interface import Producer
from TwitterUserCapture.interaction.HbaseInterface import HbaseInterface


# 封装了连接数据库，朝数据库和kafka发送消息的功能
class DataStorage(object):
    def __init__(self, mongo_list, hbase_list=None):
        """
        初始化：连接mongo数据库和kafka
        :param mongo_list: mongo数据库列表，类型list
            格式：[{'host': 'ip:port', 'db': 'database_name', 'cl': 'collection_name'}, {...}, ...]
        :param kafka_list: kafka列表，类型list
            格式：[{'brokers': 'ip1:port1,ip2:port2,...', 'topic': 'topic_name'}, {...}, ...]
        :return: 无
        """
        self.collections = {}
        self.producers = {}
        self.hbaseThrift = None
        if hbase_list:
            self.hbaseThrift = hbase_list[0]
        self.hbaseInterface = hbase_list
        self.createUniqueIndex(mongo_list)
        self.update_collections(mongo_list)
        # if hbase_list:
            # self.updateHbaseInterface(hbase_list)

    def createUniqueIndex(self,mongoList):
        for i in range(len(mongoList)):
            client = MongoClient(mongoList[i].get('host'))
            if mongoList[i].get('cl') not in client[mongoList[i].get('db')].collection_names():
                client[mongoList[i].get('db')][mongoList[i].get('cl')].ensure_index('id', unique=True)

    def update_collections(self, mongo_list):
        """
        连接mongo_list中的mongo数据表
        :param mongo_list: mongo数据库列表，类型list
            格式：[{'host': 'ip:port', 'db': 'database_name', 'cl': 'collection_name'}, {...}, ...]
        :return:
        """
        [self.collections.update(
            {mongo.get('name', mongo.get('cl')): MongoClient(mongo.get('host'), readPreference='secondaryPreferred').
                get_database(mongo.get('db')).get_collection(mongo.get('cl'))})
         for mongo in mongo_list]

    def update_producers(self, kafka_list):
        """
        连接kafka_list中的topic
        :param kafka_list: kafka列表，类型list
            格式：[{'brokers': 'ip1:port1,ip2:port2,...', 'topic': 'topic_name'}, {...}, ...]
        :return:
        """
        [self.producers.update({producer.get("topic"): Producer(producer.get("brokers"), producer.get("topic"))}) for
         producer in kafka_list]

    def updateHbaseInterface(self, hbaseThriftList):
        """

        :param habseList:
        :return:
        """
        [self.hbaseInterface.update({hbaseThrift.get("tableName"): HbaseInterface(hbaseThrift.get("host"),hbaseThrift.get("port"),hbaseThrift.get("tableName"))}) for
         hbaseThrift in hbaseThriftList]

    def run(self, cl_name, command_type, *args, **kwargs):
        """
        朝数据库和kafka发送相应请求
        :param cl_name: collection_name, 目标数据表名
        :param command_type: 命令类型，目前有'update', 'insert', 'remove'三种操作
        :param args: 命令，根据类型而有所不同，与mongo命令相同
        :param kwargs: 命令，根据类型而有所不同，与mongo命令相同，包括upsert和multi两种可选参数
        :return: 无
        """
        if cl_name not in self.collections.keys():
            raise ValueError('can not find collection "%s"' % cl_name)
        # if topic and topic in self.producers:
        #     self.__send_message(topic, cl_name, command_type, *args, **kwargs)
        #     pass
        if command_type == 'insert':
            assert len(args) == 1, 'value number error'
            try:
                self.collections.get(cl_name).insert(args[0], **kwargs)
            except pymongo.errors.DuplicateKeyError:
                pass
            except pymongo.errors.BulkWriteError:
                pass
        elif command_type == 'update':
            assert len(args) == 2, 'value number error'
            assert isinstance(args[0], dict) and isinstance(args[1], dict), 'value error'
            try:
                self.collections.get(cl_name).update(args[0], args[1], **kwargs)
            except pymongo.errors.DuplicateKeyError:
                pass
        elif command_type == 'remove':
            assert len(args) == 1, 'value number error'
            assert isinstance(args[0], dict), 'value error'
            self.collections.get(cl_name).remove(args[0], **kwargs)
        else:
            raise ValueError('command "%s" is unsupported.' % command_type)

    # 插入操作
    def insert(self, cl_name, *args, **kwargs):
        self.run(cl_name, 'insert', *args, **kwargs)

    # 更新操作
    def update(self, cl_name, *args, **kwargs):
        self.run(cl_name, 'update', *args, **kwargs)

    # 删除操作
    def remove(self,  cl_name, *args, **kwargs):
        self.run(cl_name, 'remove', *args, **kwargs)

    # 插入操作（不发kafka）
    def insert_local(self, cl_name, *args, **kwargs):
        self.run(cl_name, 'insert', *args, **kwargs)

    # 更新操作（不发kafka）
    def update_local(self, cl_name, *args, **kwargs):
        self.run(cl_name, 'update', *args, **kwargs)

    # 删除操作（不发kafka）
    def remove_local(self, cl_name, *args, **kwargs):
        self.run(cl_name, 'remove', *args, **kwargs)

    # 返回名称为cl_name的collection实例
    def get(self, cl_name):
        return self.collections.get(cl_name)

    def getHbase(self, hbaseThriftName):
        return self.hbaseInterface.get(hbaseThriftName)

    # 往kafka发送数据
    def __send_message(self, topic, collection_name, *args, **kwargs):
        try:
            # message = '\"%s\"' % str({collection_name: list(args) + [kwargs]}). \
            #     replace('"', '\\\"').replace('`', '\`').replace('$', '\$')
            message = {collection_name: list(args) + [kwargs]}
            self.producers.get(topic).send_data(message)
        except Exception as e:
            print e.message
            pass

    # 往kafka发送图片
    def send_picture(self, topic, picture_name, data, collection_name, *args, **kwargs):
        try:
            picture = {"name": picture_name, "data": data, collection_name: list(args) + [kwargs]}
            self.producers.get(topic).send_data(picture)
        except Exception as e:
            print e.message
            pass
