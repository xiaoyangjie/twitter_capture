# -*- coding:utf-8 -*-
import sys
sys.path.insert(0, '/home/server-cj/twitter/Django')
from TwitterTweetCapture.common.decorator import *
from TwitterTweetCapture.api.API import *
from TwitterTweetCapture.common.tools import createCollection

@setLogging()
def filterRealtimeTweet(tweetHost=None, tweetDatabase=None, tweetCollection=None,
                 proxyList=None,ids=None, keywords=None, locations=None,classificationName=None):
    """

    :param tweetHost:
    :param tweetDatabase:
    :param tweetCollection:
    :param proxyList:
    :param ids:
    :param keywords:
    :param locations:
    :return:
    """
    createCollection(tweetHost, tweetDatabase, tweetCollection,'tweet')
    mul_api = MultiProcessAPI(proxy_list=proxyList)
    mul_api.get_filter_tweet(tweet_collection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection,
                                               'name': 'tweet'},
                             ids=ids, keywords=keywords, locations=locations, classificationName=classificationName
                             )  # ids

if __name__ == '__main__':
    # tweetHost = 'mongodb://222.197.180.245'
    tweetHost = 'mongodb://mongo:123456@121.49.99.14'        # 1
    # tweetHost = 'localhost'
    tweetDatabase = 'KeywordsTweets'
    tweetCollection = 'Terrorism'

    proxyList = ['192.168.1.148:8111']       # 本机ip
    # proxyList = ['127.0.0.1:59998']
    # keywords = ['shoot,shooting']
    # keywords = ['terrorism,terrorist']
    # keywords = ['dead,die,died']
    # keywords = ['kill,killed,bomb']
    keywords = ['terrorism,kill, dead, bomb, shoot,shooting,die,died,terrorist,attack,attacked']
    # classificationName = 'terrorism'
    filterRealtimeTweet(tweetHost=tweetHost, tweetDatabase=tweetDatabase, tweetCollection=tweetCollection,
                        proxyList=proxyList, keywords=keywords)

