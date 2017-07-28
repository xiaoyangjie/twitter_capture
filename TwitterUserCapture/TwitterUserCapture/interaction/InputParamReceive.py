# -*- coding: UTF-8 -*-
"""
__author__ = 'YJ'

create_at :  2017/3/13

Function:
    实现接收采集程序的参数

History :
    first update time :

"""
import json


class InputParamReceive():

    def __init__(self):
        pass

    @staticmethod
    def userAvatarParam():
        flag = False
        configFile = open('D:/YjProject/TwitterUserCapture/UserAvatar.json', 'rb')
        userHost = None
        userDatabase = None
        userCollection = None
        proxyList = None
        hbaseThriftHost = None
        hbaseThriftPort = None
        hbaseTableName = None
        picture_type = None
        param = json.load(configFile)
        userHost = param.get('userHost')
        userDatabase = param.get('userDatabase')
        userCollection = param.get('userCollection')
        proxyList = param.get('proxyList')
        hbaseThriftHost = param.get('hbaseThriftHost')
        hbaseThriftPort = param.get('hbaseThriftPort')
        hbaseTableName = param.get('hbaseTableName')
        picture_type = param.get('picture_type')
        if userHost == None or userDatabase == None or userCollection == None:
            print "please input correct userHost and userDatabase and userCollection"
        elif hbaseThriftHost == None or hbaseThriftPort == None or hbaseTableName == None:
            print "please input correct hbaseThriftHost and hbaseThriftPort and hbaseTableName"
        elif proxyList == None:
            print "please input correct proxyList"
        elif picture_type != 'avatar' and picture_type != 'banner':
            print "please input correct pictureType"
        else:
            flag = True
        return flag

    @staticmethod
    def userInfoParam():
        flag = False
        configFile = open('D:/YjProject/TwitterUserCapture/UserInfo.json', 'rb')
        userHost = None
        userDatabase = None
        userCollection = None
        proxyList = None
        accountIdList = None
        screenNameList = None
        param = json.load(configFile)
        userHost = param.get('userHost')
        userDatabase = param.get('userDatabase')
        userCollection = param.get('userCollection')
        proxyList = param.get('proxyList')
        accountIdList = param.get('accountIdList')
        screenNameList = param.get('screenNameList')
        if userHost == None or userDatabase == None or userCollection == None:
            print "please input correct userHost and userDatabase and userCollection"
        elif proxyList == None:
            print "please input correct proxyList"
        else:
            flag = True
        return flag

    @staticmethod
    def userFriendsParam():
        flag = False
        configFile = open('D:/YjProject/TwitterUserCapture/UserFriends.json', 'rb')
        userHost = None
        userDatabase = None
        userCollection = None
        userFriendsHost = None
        userFriendsDatabase = None
        userFriendsCollection = None
        proxyList = None
        accountIdList = None
        screenNameList = None
        param = json.load(configFile)
        userHost = param.get('userHost')
        userDatabase = param.get('userDatabase')
        userCollection = param.get('userCollection')
        userFriendsHost = param.get('userFriendsHost')
        userFriendsDatabase = param.get('userFriendsDatabase')
        userFriendsCollection = param.get('userFriendsCollection')
        proxyList = param.get('proxyList')
        accountIdList = param.get('accountIdList')
        screenNameList = param.get('screenNameList')
        if userHost == None or userDatabase == None or userCollection == None:
            print "please input correct userHost and userDatabase and userCollection"
        elif userFriendsHost == None or userFriendsDatabase == None or userFriendsCollection == None:
            print "please input correct userFriendsHost and userFriendsDatabase and userFriendsCollection"
        elif proxyList == None:
            print "please input correct proxyList"
        else:
            flag = True
        return flag

    @staticmethod
    def userFollowersParam():
        flag = False
        configFile = open('D:/YjProject/TwitterUserCapture/UserFollowers.json', 'rb')
        userHost = None
        userDatabase = None
        userCollection = None
        userFollowersHost = None
        userFollowersDatabase = None
        userFollowersCollection = None
        proxyList = None
        accountIdList = None
        screenNameList = None
        param = json.load(configFile)
        userHost = param.get('userHost')
        userDatabase = param.get('userDatabase')
        userCollection = param.get('userCollection')
        userFollowersHost = param.get('userFollowersHost')
        userFollowersDatabase = param.get('userFollowersDatabase')
        userFollowersCollection = param.get('userFollowersCollection')
        proxyList = param.get('proxyList')
        accountIdList = param.get('accountIdList')
        screenNameList = param.get('screenNameList')
        if userHost == None or userDatabase == None or userCollection == None:
            print "please input correct userHost and userDatabase and userCollection"
        elif userFollowersHost == None or userFollowersDatabase == None or userFollowersCollection == None:
            print "please input correct userFollowersHost and userFollowersDatabase and userFollowersCollection"
        elif proxyList == None:
            print "please input correct proxyList"
        else:
            flag = True
        return flag