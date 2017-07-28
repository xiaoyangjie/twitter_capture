# -*- coding:utf-8 -*-
import sys
sys.path.insert(0, '/home/server-cj/twitter/Django')
from TwitterTweetCapture.api.API import *
from TwitterTweetCapture.common.decorator import *
from TwitterTweetCapture.common.tools import createCollection

# @status_collection(with_log=True)
@setLogging()
def realtimeTweet(userHost=None, userDatabase=None, userCollection=None,
                   tweetHost=None, tweetDatabase=None, tweetCollection=None,proxyList=PROXYLIST):
    """
    功能：重点用户实时关注
    :param userHost:
    :param userDatabase:
    :param userCollection:
    :param tweetHost:
    :param tweetDatabase:
    :param tweetCollection:
    :param proxyList:
    :return:
    """
    while True:
        createCollection(tweetHost, tweetDatabase, tweetCollection, 'tweet')
        mul_api = MultiProcessAPI(proxy_list=proxyList)
        mul_api.get_realtime_tweet(tweet_collection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection, 'name': 'tweet'},
                                   user_collection={'host': userHost, 'db': userDatabase, 'cl': userCollection,
                                                    'name': 'user'}
                                   )
        print "finish"

if __name__ == '__main__':
    userHost = 'mongodb://mongo:123456@121.49.99.14'     # 1
    # userHost = 'mongodb://222.197.180.245'
    userDatabase = 'twitter_user_tweet'
    userCollection = 'classified_user'

    # tweetHost = 'mongodb://222.197.180.245'
    tweetHost = 'mongodb://mongo:123456@121.49.99.14'     # 1
    # tweetHost = 'localhost'
    tweetDatabase = 'RealtimeTweets'
    tweetCollection = 'tweets_20170527'

    realtimeTweet(userHost=userHost, userDatabase=userDatabase, userCollection=userCollection,
                   tweetHost=tweetHost, tweetDatabase=tweetDatabase, tweetCollection=tweetCollection)