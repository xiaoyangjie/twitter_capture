import pymongo
import time

cli = pymongo.MongoClient('mongodb://mongo:123456@121.49.99.14')['twitter_user_tweet']['user']
cli1 = pymongo.MongoClient('mongodb://mongo:123456@121.49.99.14')['twitter_user_tweet']['userGai']

# for i in cli.find({'id':{'$exists':False}}):
#     try:
#         cli.update({'account_id':i['account_id']},{'$set':{'id':i['account_id']}})
#     except:pass
# for i in cli.find({'useYJ':{'$exists':False}}):
#     i['id'] = i['account_id']
#     del i['account_id']
#     try:
#         cli1.insert(i)
#         cli.update({'account_id':i['id']},{'$set':{'useYJ': True}})
#     except:pass
