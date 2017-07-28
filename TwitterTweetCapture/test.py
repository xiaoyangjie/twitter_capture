
# from TwitterTweetCapture.api.API import API
# API().rest_find_one_tweet(841482821310980096)
import requests
proxies = {'http':'http://192.168.178.151:9090'}

result = requests.get('http://www.google.com',proxies=proxies)
#     result.raise_for_status()
#     print result.status_code
# except requests.RequestException as err:
#     print err.message

# import pymongo
# cli = pymongo.MongoClient('mongodb://mongo:123456@222.197.180.150')['twitter_user_tweet']['user']
# cli1 = pymongo.MongoClient('mongodb://mongo:123456@222.197.180.150')['twitter_user_tweet']['user_1']
# for i in cli.find():
#     i['id'] = i['account_id']
#     cli1.insert(i)