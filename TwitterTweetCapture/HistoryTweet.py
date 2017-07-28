import sys
sys.path.insert(0, '/home/server-cj/twitter/Django')
from TwitterTweetCapture.api.API import *
from TwitterTweetCapture.common.decorator import *
from TwitterTweetCapture.common.tools import createCollection

@setLogging()
def historyTweet(tweetHost=None, tweetDatabase=None, tweetCollection=None,
                  userHost=None, userDatabase=None, userCollection=None,
                  proxyList=None,screenNameList=None, userIdList=None, count=200,
                 requestNum=0):
    """

    :param tweetHost:
    :param tweetDatabase:
    :param tweetCollection:
    :param userHost:
    :param userDatabase:
    :param userCollection:
    :param proxyList:
    :param screenNameList:
    :param userIdList:
    :param count:
    :param requestNum:
    :return:
    """
    createCollection(tweetHost, tweetDatabase, tweetCollection, 'tweet')
    mul_api = MultiProcessAPI(proxy_list=proxyList)
    mul_api.getUserHistory(tweetCollection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection, 'name': 'tweet'},
                             userCollection={'host': userHost, 'db': userDatabase, 'cl': userCollection,
                                              'name': 'user'},
                             screenNameList=screenNameList, userIdList=userIdList,count=count, requestNum=requestNum)

if __name__ == '__main__':
    # tweetHost = 'mongodb://222.197.180.245'
    tweetHost = 'mongodb://mongo:123456@121.49.99.14'    # 1
    tweetDatabase = 'twitter_user_tweet'
    # tweetDatabase = 'HistoryTweets'
    tweetCollection = 'user_tweets'

    # userHost = 'mongodb://222.197.180.245'
    userHost = 'mongodb://mongo:123456@121.49.99.14'       # 1
    userDatabase = 'twitter_user_tweet'
    userCollection = 'user'    # 'classified_user'

    proxyList = ['192.168.1.148:8111']

    screenNameList = None
    accountIdList = None

    historyTweet(tweetHost=tweetHost,tweetDatabase=tweetDatabase,tweetCollection=tweetCollection,
                 userHost=userHost, userDatabase=userDatabase, userCollection=userCollection,
                  proxyList=proxyList,screenNameList=screenNameList,userIdList=accountIdList,
                 count=100,requestNum=2)