import sys
sys.path.insert(0, '/home/server-cj/twitter/Django')
from TwitterTweetCapture.api.API import MultiProcessAPI
from TwitterTweetCapture.common.decorator import *

# @status_collection(with_log=True)
@setLogging()
def tweetPictureDownload(tweetHost=None, tweetDatabase=None, tweetCollection=None,
                   proxyList=None,hbaseThriftHost=None ,hbaseThriftPort=None,
                   hbaseTableName=None, type=None):
    """

    :param tweetHost:
    :param database:
    :param collection:
    :param proxy_list:
    :param img_path:
    :param vid_path:
    :param info_type:
    :return:
    """
    mul_api = MultiProcessAPI(proxy_list=proxyList)
    mul_api.tweetPictureDownload(tweet_collection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection,
                                               'name': 'tweet'}, type=type,
                                 hbase= {'host': hbaseThriftHost, 'port': hbaseThriftPort, 'tableName': hbaseTableName}
                             )

if __name__ == '__main__' :
    tweetHost = '192.168.178.101'
    tweetDatabase = 'yj'
    tweetCollection = 'FilterTweetUK'
    type = 'img'
    proxyList = ['192.168.178.45:9090']
    tweetPictureDownload(tweetHost=tweetHost,tweetDatabase=tweetDatabase,
                         tweetCollection=tweetCollection,type=type,proxyList=proxyList)
