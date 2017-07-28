# coding=utf-8
"""
author  : YJ

create_at : 2017/3/8

History:
    first update time : 2017/3/8
"""

import pymongo

client = pymongo.MongoClient('192.168.178.101')['twitter_user_tweet']['classified_user']
clientUpdate = pymongo.MongoClient('192.168.178.218')['test']['ClassifiedUser']
# clientUpdate.ensure_index('id', unique=True)
for i in client.find({},{'_id':0,'classification':1,'account_id':1}):
    clientUpdate.update({'id':i.get('account_id')},{'$set': {'classification':i.get('classification')}},upsert=True)
