import sys
sys.path.insert(0, '/home/server-cj/twitter/Django')
from TwitterTweetCapture.download_tweet_info.crawler import Crawler
from TwitterTweetCapture.common.decorator import *

# @status_collection(with_log=True)
@setLogging()
def tweetPictureDownload(tweetHost=None, tweetDatabase=None, tweetCollection=None,
                   proxyList=None,hbaseThriftHost='192.168.178.212' ,hbaseThriftPort=9090,
                   hbaseTableName='imagetable', info_type=None):
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
    if proxyList is not None and isinstance(proxyList, list):
        proxy_host = str(proxyList[0].split(':')[0])
        proxy_port = int(proxyList[0].split(':')[1])
        crawler = Crawler(proxy_host=proxy_host, proxy_port=proxy_port,
                          tweet_collection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection, 'name': 'tweet'},
                          hbaseThrift={'host': hbaseThriftHost, 'port': hbaseThriftPort, 'tableName': hbaseTableName, 'name': 'table'}
                          )
    else:
        crawler = Crawler(tweet_collection={'host': tweetHost, 'db': tweetDatabase, 'cl': tweetCollection, 'name': 'tweet'},
                          hbaseThrift={'host': hbaseThriftHost, 'port': hbaseThriftPort, 'tableName': hbaseTableName, 'name': 'table'}
                          )
    crawler.getTweetInfo(info_type=info_type)

if __name__ == '__main__' :
    tweetHost = 'mongodb://192.168.178.218:27017,192.168.178.220:27017/?replicaSet=shard1'
    tweetDatabase = 'test'
    tweetCollection = 'animation'
    info_type = 'img'
    tweetPictureDownload(tweetHost=tweetHost,tweetDatabase=tweetDatabase,
                         tweetCollection=tweetCollection,img_path=img_path,info_type=info_type)
