# -*- coding:utf-8 -*-
# python2.7
# user_followers.py
# 获取用户的followers列表
import json
from TwitterUserCapture.api import *
from TwitterUserCapture.common.decorator import *

# MONGOHOST = 'mongodb://mongo:123456@222.197.180.150'
MONGOHOST = 'mongodb://mongo:123456@121.49.99.14'
@set_logging()
def userFollowers(userHost=MONGOHOST, userDatabase="twitter_user_tweet", userCollection='user',
                  userFollowersHost=MONGOHOST, userFollowersDatabase="twitter_user_tweet", userFollowersCollection='user',
                 proxyList=None, accountIdList=None, screenNameList=None):
    """
    封装的采集函数，获取用户的followers列表
    :param userHost: 用户信息数据表的host，类型string，例子："192.168.178.108"
    :param userDatabase: 用户信息数据表的database，类型string，例子："twitter_user"
    :param userCollection: 用户信息数据表的collection，类型string，例子："twitter_user"
    :param userFollowersHost: 用户粉丝数据表的host，类型string，例子："192.168.178.108"
    :param userFollowersDatabase: 用户粉丝数据表的database，类型string，例子："twitter_user"
    :param userFollowersCollection: 用户粉丝数据表的collection，类型string，例子："twitter_user"
    :param proxyList: 代理，类型string，例子：'192.168.178.149:9090'
    :param accountIdList: 待查询用户ID列表，类型list，例子：[12, 13, 14]，默认为None
    :param screenNameList: 待查询用户screen_name列表，类型list，例子：['bbc', 'cnn']，默认为None
    当accountIdList和screenNameList都为None时，从用户信息数据表中查找未更新过的用户进行更新
    :return:
    """
    mongo_list = [{'host': userHost, 'db': userDatabase, 'cl': userCollection},
                  {'host': userFollowersHost, 'db': userFollowersDatabase, 'cl': userFollowersCollection},
                  {'host': userHost, 'db': 'crawler_statues', 'cl': 'twitter_apis', 'name': 'token'}]
    data_storage = DataStorage(mongo_list=mongo_list)
    twitter = MultiProcessAPI(proxyList=proxyList, data_storage=data_storage, debug=False)
    result = True
    while result:
        result = twitter.get_users_follow(user_type="followers",
                                          update_cl_name=userCollection,
                                          upsert_cl_name=userFollowersCollection,
                                          account_id_list=accountIdList,
                                          screen_name_list=screenNameList,
                                          max_list_num=)


if __name__ == "__main__":
    userFollowers(proxyList=['192.168.1.148:8111'])  # ,screenNameList=['xiaoyangjie3'])
