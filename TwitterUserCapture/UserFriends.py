# -*- coding:utf-8 -*-
# python2.7
# user_friends.py
# 获取用户的friends列表
import json

from TwitterUserCapture.api import *
from TwitterUserCapture.common.decorator import *
MONGOHOST = 'mongodb://mongo:123456@121.49.99.14'

@set_logging()
def userFriends(userHost=MONGOHOST, userDatabase="twitter_user_tweet", userCollection='user',
                 userFriendsHost=MONGOHOST, userFriendsDatabase="twitter_user_tweet", userFriendsCollection='user',
                 proxyList=None, accountIdList=None, screenNameList=None):
    """
    封装的采集函数，获取用户的friends列表
    :param userHost: 用户信息数据表的host，类型string，例子："192.168.178.108"
    :param userDatabase: 用户信息数据表的database，类型string，例子："twitter_user"
    :param userCollection: 用户信息数据表的collection，类型string，例子："twitter_user"
    :param userFriendsHost: 用户朋友数据表的host，类型string，例子："192.168.178.108"
    :param userFriendsDatabase: 用户朋友数据表的database，类型string，例子："twitter_user"
    :param userFriendsCollection: 用户朋友数据表的collection，类型string，例子："twitter_user"
    :param proxyList: 代理，类型string，例子：['192.168.178.149:9090']
    :param accountIdList: 待查询用户ID列表，类型list，例子：[12, 13, 14]，默认为None
    :param screenNameList: 待查询用户screen_name列表，类型list，例子：['bbc', 'cnn']，默认为None
    当accountIdList和screenNameList都为None时，从用户信息数据表中查找未更新过的用户进行更新
    :return:
    """
    mongo_list = [{'host': userHost, 'db': userDatabase, 'cl': userCollection},
                  {'host': userFriendsHost, 'db': userFriendsDatabase, 'cl': userFriendsCollection},
                  {'host': userHost, 'db': 'crawler_statues', 'cl': 'twitter_apis', 'name': 'token'}]
    data_storage = DataStorage(mongo_list=mongo_list)
    twitter = MultiProcessAPI(proxyList=proxyList, data_storage=data_storage, debug=False)
    result = True
    while result:
        result = twitter.get_users_follow(user_type="friends",
                                          update_cl_name=userCollection,
                                          upsert_cl_name=userFriendsCollection,
                                          account_id_list=accountIdList,
                                          screen_name_list=screenNameList)

if __name__ == "__main__":
    userFriends(proxyList=['192.168.1.148:8111'])

    """
    configFile = open('D:/YjProject/TwitterUserCapture/UserFriends.json', 'rb')
    userHost = None
    userDatabase = None
    userCollection = None
    userFriendsHost = None
    userFriendsDatabase = None
    userFriendsCollection = None
    proxyList = None
    accountIdList = None
    screenNameList = None
    param = json.load(configFile)
    userHost = param.get('userHost')
    userDatabase = param.get('userDatabase')
    userCollection = param.get('userCollection')
    userFriendsHost = param.get('userFriendsHost')
    userFriendsDatabase = param.get('userFriendsDatabase')
    userFriendsCollection = param.get('userFriendsCollection')
    proxyList = param.get('proxyList')
    accountIdList = param.get('accountIdList')
    screenNameList = param.get('screenNameList')
    if userHost == None or userDatabase == None or userCollection == None:
        print "please input correct userHost and userDatabase and userCollection"
    elif userFriendsHost == None or userFriendsDatabase == None or userFriendsCollection == None:
        print "please input correct userFriendsHost and userFriendsDatabase and userFriendsCollection"
    elif proxyList == None:
        print "please input correct proxyList"
    else:
        userFriends(proxyList=proxyList,userHost=userHost, userDatabase=userDatabase, userCollection=userCollection,
                  userFriendsHost=userFriendsHost, userFriendsDatabase=userFriendsDatabase,
                    userFriendsCollection=userFriendsCollection, accountIdList=accountIdList,
                    screenNameList=screenNameList)"""
