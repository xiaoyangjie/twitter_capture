# -*- coding:utf-8 -*-
"""
__author__ = 'YJ'
create_time = '2017.3.7'
# python2.7
# user_info.py
# 获取用户的信息
"""
import json
from argparse import *

import pymongo

from TwitterUserCapture.api import *
from TwitterUserCapture.common.decorator import *
import logging

@set_logging()
def userInfo(userHost="192.168.178.108", userDatabase="twitter_user", userCollection='twitter_user',
              proxyList=None, accountIdList=None, screenNameList=None):
    """
    封装的采集函数，获取用户的信息
    :param userHost: 用户信息数据表的host，类型string，例子："192.168.178.108"
    :param userDatabase: 用户信息数据表的database，类型string，例子："twitter_user"
    :param userCollection: 用户信息数据表的collection，类型string，例子："twitter_user"
    :param proxyList: 代理，类型string，例子：['192.168.178.149:9090']
    :param accountIdList: 待查询用户ID列表，类型list，例子：[12, 13, 14]，默认为None
    :param screenNameList: 待查询用户screen_name列表，类型list，例子：['bbc', 'cnn']，默认为None
    当accountIdList和screenNameList都为None时，从用户信息数据表中查找未更新过的用户进行更新
    :return:
    """
    mongo_list = [{'host': userHost, 'db': userDatabase, 'cl': userCollection},
                  {'host': userHost, 'db': 'crawler_statues', 'cl': 'twitter_apis', 'name': 'token'}]
    data_storage = DataStorage(mongo_list=mongo_list)
    twitter = MultiProcessAPI(proxyList=proxyList, data_storage=data_storage, debug=False)
    result = True
    while result:
        result = twitter.get_users_lookup(cl_name=userCollection,
                                          account_id_list=accountIdList,
                                          screen_name_list=screenNameList)
        # if not result:
        #     #如果没有可以采集的用户暂定3个小时，继续采集。
        #     logging.warning('maybe UserFriend.py and UserFollowers.py procedure stop, please restart UserFriends.py and UserFollowers.py ')
        #     time.sleep(3*60*60)
        #     result = True


def getScreenName():
    cli = pymongo.MongoClient('mongodb://mongo:123456@121.49.99.14')['zwy']['SpamUser']
    # fp = open('C:/Users/yj/Desktop/bitvise/diff_type_tweets/think_tank_tweets.txt')
    result = []
    # for i in fp.readlines():
    #    print i.split('word')[0].split(':')[1].strip('\t')
    #    result.append(i.split('word')[0].split(':')[1].strip('\t'))
    for i in cli.find():
        try:
            result.append(i['screen_name'])
        except: pass
    return result


if __name__ == "__main__":
    userHost = 'mongodb://mongo:123456@121.49.99.14'  # 'mongodb://mongo:123456@222.197.180.150'
    userDatabase = 'twitter_user_tweet'   # 'zwyTemp'
    userCollection = 'user'  # 'User'
    proxyList = ['192.168.1.148:8111']
    # accountIdList = None
    # screenNameList = getScreenName()

    userInfo(userHost=userHost, userDatabase=userDatabase, userCollection=userCollection,
              proxyList=proxyList,screenNameList=None)