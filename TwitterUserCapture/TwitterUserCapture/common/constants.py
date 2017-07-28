# -*- coding:utf-8 -*-
# python2.7
# constants.py
# 常量定义文件


INFINITE = -1  # 无采集上限
PROXY = '127.0.0.1:8118'  # 代理
# HOST = 'mongodb://mongo:123456@222.197.180.150'  # mongo host
HOST = 'mongodb://mongo:123456@121.49.99.14'
BASE_PATH = "/home/server-cj/twitter/"  # Linux图片存储路径
WIN_BASE_PATH = "D:/twitter/"  # Windows图片存储路径

COOKIES_COLLECTION = {'host': HOST, 'db': 'twitter_cookie', 'cl': 'cookies'}  # Twitter的cookie数据表

# friends/followers列表采集临时记录数据表
RECORD_COLLECTION = {'host': HOST, 'db': 'crwaler_statuses', 'cl': 'temp_record', 'name': 'record'}

# Twitter API的token数据表
TOKEN_COLLECTION = {'host': HOST, 'db': 'crawler_statues', 'cl': 'twitter_apis', 'name': 'token'}  # 'db': 'token'

# 爬虫程序的HTTP头部
HTTP_HEADER = {'Connection': 'Keep-Alive',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36',
               }

API_FOLLOW_LIMIT = 5000  # friends/followers列表单次采集数量上限
API_FOLLOW_TIME_LIMIT = 15  # 每15分钟，同一个token的friends/followers列表采集次数上限
API_SEARCH_LIMIT = 20  # 单次查询数量上限
API_LOOKUP_LIMIT = 100  # 单次查询用户信息的用户数上限
API_LOOKUP_TIME_LIMIT = 180  # 每15分钟，同一个token的用户信息采集次数上限
MAX_FOLLOW = 5000  # 每个用户所需采集的friends/followers列表长度上限
PROCESS_COUNT = 8  # 启动的线程数

UPDATE_TIME = 1481422944  # 时间戳，位于该时间之前的用户需重新采集用户信息
