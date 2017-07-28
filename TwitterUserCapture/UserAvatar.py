# -*- coding:utf-8 -*-
# python2.7
# user_avatar.py
# 获取用户的头像
import json
from TwitterUserCapture.api import *
from TwitterUserCapture.common.decorator import *
from TwitterUserCapture.common.constants import *


@set_logging()
def userAvatar(userHost="192.168.178.108", userDatabase="twitter_user", userCollection='twitter_user',
                proxyList=None, hbaseThriftHost='192.168.178.212' ,hbaseThriftPort=9090,
               hbaseTableName='linuxtest',base_path=WIN_BASE_PATH, picture_type='avatar'):
    """
    封装的采集函数，获取用户的头像
    :param host: 用户信息数据表的host，类型string，例子："192.168.178.108"
    :param database: 用户信息数据表的database，类型string，例子："twitter_user"
    :param collection: 用户信息数据表的collection，类型string，例子："twitter_user"
    :param proxy: 代理，类型string，例子：'192.168.178.149:9090'
    :param
    :return:
    """
    mongo_list = [{'host': userHost, 'db': userDatabase, 'cl': userCollection},
                  {'host': userHost, 'db': 'crawler_statues', 'cl': 'twitter_apis', 'name': 'token'}]
    hbaseThriftList = [{'host':hbaseThriftHost, 'port':hbaseThriftPort, 'tableName':hbaseTableName}]
    data_storage = DataStorage(mongo_list=mongo_list, hbase_list=hbaseThriftList)
    twitter = MultiProcessAPI(proxyList=proxyList, data_storage=data_storage)
    result = True
    while result:
        result = twitter.get_users_picture(cl_name=userCollection, base_path=base_path, tableName=hbaseTableName,
                                           picture_type=picture_type)


if __name__ == "__main__":


    userAvatar(proxyList=proxyList,userHost=userHost,
                userDatabase=userDatabase ,userCollection=userCollection,
                 hbaseThriftHost=hbaseThriftHost,hbaseThriftPort=hbaseThriftPort,
                 hbaseTableName=hbaseTableName,  picture_type=picture_type)
