# -*- coding: UTF-8 -*-
# python2.7
# rest_api.py
# Twitter API封装模块
from TwitterAPI import TwitterAPI
from TwitterAPI.TwitterError import TwitterConnectionError
from TwitterAPI.TwitterError import TwitterRequestError
import time
from multiprocessing.dummy import Pool
from TwitterUserCapture.common.data_storage import DataStorage
from TwitterUserCapture.interaction.HbaseInterface import HbaseInterface
from TwitterUserCapture.common.constants import *
from TwitterUserCapture.common.tools import *
import logging
import os
import requests
import random


# 调用REST_API发送一次请求的封装
class API(object):
    def __init__(self, data_storage, proxy=None, token=None):
        """
        初始化：设置代理、数据库、kafka、token
        :param proxy: 设置代理，类型string，格式：'ip:port'
        :param data_storage: 设置数据库，类型DataStorage，见data_storage.py
        :param token: 设置token用于登录API，类型dict，格式：
            {'consumer_key': '', 'consumer_secret': '', 'access_token': '', 'access_token_secret': ''}
        :return: 无
        """
        self.__proxy = proxy
        # 创建/连接数据库
        if data_storage.hbaseThrift:
            self.__hbaseInterface = HbaseInterface(data_storage.hbaseThrift.get('host'),data_storage.hbaseThrift.get('port'),
                                               data_storage.hbaseThrift.get('tableName'))
        self.__data_storage = data_storage
        self.__data_storage.update_collections([RECORD_COLLECTION])
        self.__token = token or {}
        self.__api = None
        self.set_api()

    def set_proxy(self, proxy='127.0.0.1:8118'):
        """
        设置代理
        :param proxy: 见初始化
        :return: 无
        """
        self.__proxy = proxy

    def set_token(self, token):
        """
        设置token
        :param token: 见初始化
        :return: 无
        """
        self.__token = token

    def set_api(self):
        """
        设置TwitterAPI（初始化以及重设了代理或token后调用）
        :return: 无
        """
        if self.__token:
            self.__api = TwitterAPI(consumer_key=self.__token.get('consumer_key'),
                                    consumer_secret=self.__token.get('consumer_secret'),
                                    access_token_key=self.__token.get('access_token'),
                                    access_token_secret=self.__token.get('access_token_secret'),
                                    proxy_url=self.__proxy)

    def get_users_lookup(self, user_list=None, with_screen_name=False, cl_name='user'):
        """
        补全用户信息
        :param user_list: 待采集详细信息的用户列表
        :param with_screen_name: 列表中是否包含screen_name
        :param cl_name: 存储结果的collection名
        :return: 无
        """
        logging.debug('start')
        account_id_list = []
        screen_name_list = []
        if not with_screen_name:
            account_id_list = user_list
        else:  # 将ID列表和screen_name列表分离
            for data in user_list:
                if is_account_id(data):
                    account_id_list.append(data)
                elif is_screen_name(data):
                    screen_name_list.append(data.lower())
        user_num = len(account_id_list) + len(screen_name_list)
        commands = {}
        if account_id_list:
            commands.update({'user_id': account_id_list})
        if screen_name_list:
            commands.update({'screen_name': screen_name_list})
        if commands:
            commands.update({'include_entities': False})
        else:
            logging.warning('there is not valid user account_id or screen_name.')
            return
        try:
            results = self.__api.request('users/lookup', commands)
            if results.status_code >= 400:
                raise TwitterRequestError(results.status_code)
        except TwitterRequestError as err:
            logging.error(err.message)
            if err.status_code == 404:  # 结果未找到
                [self.__data_storage.update(cl_name, {'id': user}, {'$set': {'alive': False}})
                 for user in account_id_list]
                [self.__data_storage.update(cl_name, {'screen_name': user}, {'$set': {'alive': False}})
                 for user in screen_name_list]
                logging.info('users: %d, success: 0' % user_num)
                if account_id_list:
                    logging.info("failed id: %s" % str(account_id_list))
                if screen_name_list:
                    logging.info("failed name: %s" % str(screen_name_list))
                return
            else:
                raise TwitterRequestError(err.status_code)
        except TwitterConnectionError as err:
            logging.error(err.message)
            raise
        else:
            try:
                for data in results:
                    result = self._load_user_info(data)
                    # 更新找到的用户
                    self.__data_storage.update(cl_name, {'id': result['id']}, {'$set': result},
                                               upsert=True)
                    # 已获取到的用户从列表中移除
                    if result['id'] in account_id_list:
                        account_id_list.remove(result['id'])
                    elif result['screen_name'].lower() in screen_name_list:
                        screen_name_list.remove(result['screen_name'].lower())
                # 更新未找到的用户
                [self.__data_storage.update(cl_name, {'id': user}, {'$set': {'alive': False}})
                 for user in account_id_list]
                [self.__data_storage.update(cl_name, {'screen_name': user}, {'$set': {'alive': False}})
                 for user in screen_name_list]
                logging.info(
                    'users: %d, success: %d' % (user_num, user_num - len(account_id_list) - len(screen_name_list)))
                if account_id_list:
                    logging.info("failed id: %s" % str(account_id_list))
                if screen_name_list:
                    logging.info("failed name: %s" % str(screen_name_list))
            except Exception as err:
                logging.error(err.message)
                pass

    def get_users_ids(self, user, user_type='friends', cursor=-1, rest_users_num=INFINITE,
                      update_cl_name=None,upsert_cl_name=None):
        """
        获取用户的关注者或被关注者的ID
        :param user: 待采集用户的ID或screen_name
        :param user_type: 采集类型
        :param cursor: 采集开始位置，从头开始时为-1，没有数据时为0
        :param rest_users_num: 剩余采集个数，默认全部采集
        :return: next_cursor, rest_users_num: 下一次采集的开始位置和剩余数量
        :param update_cl_name: 待更新列表的用户所在collection名
        :param upsert_cl_name: 待插入新用户的collection名
        :return cursor, rest_num, get_num: 下一次采集开始位置、剩余采集个数、本次采集个数
        """
        logging.debug('start get %s %s, rest: %d' % (str(user), user_type, rest_users_num))
        # 本次采集个数
        count = API_FOLLOW_LIMIT if rest_users_num == INFINITE or rest_users_num > API_FOLLOW_LIMIT \
            else rest_users_num
        commands = None
        search_type = None
        if is_account_id(user):
            commands = {'user_id': user, 'cursor': cursor, 'count': count}
            search_type = 'id'
        elif is_screen_name(user):
            commands = {'screen_name': user, 'cursor': cursor, 'count': count}
            search_type = 'screen_name'
        if not commands:
            logging.warning('%s is not valid user account_id or screen_name.' % str(user))
            return 0, 0, 0
        try:
            results = self.__api.request(user_type + '/ids', commands)
            if results.status_code >= 400:
                raise TwitterRequestError(results.status_code)
        except TwitterRequestError as err:
            if err.status_code == 401:  # 用户被保护
                self.__data_storage.update(update_cl_name, {search_type: user}, {'$set': {'protected': True}})
                logging.info('%s: %s, protected' % (search_type, str(user)))
                return 0, 0, 0
            elif err.status_code == 404:  # 用户不存在
                self.__data_storage.update(update_cl_name, {search_type: user}, {'$set': {'alive': False}})
                logging.info('%s: %s, not found' % (search_type, str(user)))
                return 0, 0, 0
            else:
                logging.error(err.message)
                raise TwitterRequestError(err.status_code)
        except TwitterConnectionError as err:
            logging.error(err.message)
            raise
        else:
            try:
                json_data = results.json()
                users_ids = json_data.get('ids')
            except Exception as err:
                logging.error(err.message)
                return cursor, rest_users_num, 0
            # 将新发现的用户id插入数据库
            if update_cl_name and len(users_ids):
                self.__data_storage. \
                    insert(update_cl_name, [{'id': user_id} for user_id in users_ids],
                           continue_on_error=True)
            # 更新用户对应类型的follow列表
            self.__data_storage.update(upsert_cl_name, {search_type: user},
                                       {'$addToSet': {user_type + '_ids': {'$each': users_ids}}},
                                       upsert=(search_type == 'id' or search_type == 'screen_name'))
            logging.info('%s: %s, type: %s, get: %d' % (search_type, str(user), user_type, len(users_ids)))
            next_cursor = json_data.get('next_cursor')
            rest_users_num = 0 if 0 < rest_users_num <= count \
                else rest_users_num - count if rest_users_num > count else rest_users_num
            return next_cursor, rest_users_num, len(users_ids)

    def get_user_pictures(self, id, url, cl_name, picture_type, tableName,base_path=BASE_PATH):
        """
        获取用户头像或背景条图片
        :param id: 待采集用户ID
        :param url: 图片的URL
        :param cl_name: 待采集用户所在的collection名
        :param picture_type: 图片类型，有'avatar'和'banner'两种
        :param base_path: 图片存储路径
        :return:
        """
        if picture_type not in ['avatar', 'banner']:
            logging.warning('%s is not valid picture type.' % picture_type)
            return False
        if not url:
            logging.warning('empty url')
            return True

        logging.debug('start get %s %s, url: %s' % (str(id), picture_type, url))
        # 获取当前日期，用于按日期存放图片到对应文件夹
        daily_dir_path = time.strftime("%Y-%m-%d", time.gmtime()) + '/'
        # 获取图片的保存名与数据表中存放路径的字段名
        if picture_type == 'avatar':
            file_name = str(id) + '.jpg'
            # path_name = 'profile_storage_path'
        else:
            file_name = str(id) + '.jpg'
            # path_name = 'banner_storage_path'
        # 最底层文件夹完整路径
        whole_path = base_path + picture_type + '/' + daily_dir_path
        # 若不存在则创建文件夹
        if not os.path.exists(whole_path):
            try:
                os.makedirs(whole_path)
            except os.error:
                pass
        proxy = {'https': 'http://%s' % self.__proxy}
        try:
            result = requests.get(url, timeout=60, proxies=proxy)
            result.raise_for_status()
        except requests.RequestException as err:
            logging.error(err.message)
            raise
        else:
            # 本地保存图片
            # image_file = open(whole_path + file_name, 'wb')
            # image_file.write(result.content)
            # image_file.close()
            try:
                # self.__data_storage.getHbase(tableName).sendPicture(key=file_name, data=result.content)
                self.__hbaseInterface.sendPicture(key=file_name, data=result.content)
            except Exception as e:
                logging.error(e.message)
                pass
            else :
                # 本地数据库更新图片存储路径信息
                self.__data_storage.update_local(cl_name, {'id': id},{'$set':{picture_type + '_captured':True}})
                # 往kafka发送图片以及对应的数据库操作
                # if topic:
                #     self.__data_storage. \
                #         send_picture(topic, file_name, result.content, cl_name, {'account_id': account_id},
                #                      {'$set': {path_name: "%s%s%s" % (
                #                          "win/" if base_path == WIN_BASE_PATH else "", daily_dir_path, file_name)}})
                logging.info('got avatar ' + file_name)

    def users_search(self, keyword, page=1, count=API_SEARCH_LIMIT, cl_name='user', topic=None):
        """
        根据关键词搜索用户
        :param topic: kafka的topic，为None时不发往kafka
        :param keyword: 关键词
        :param page: 搜索开始页数（从1开始）
        :param count: 每次返回结果条数（最多20条）
        :return: page：下一次搜索开始页数（为-1表示搜索结束）
        :param cl_name: 存放结果的collection名
        """
        logging.debug('start search %s' % keyword)
        if count > API_SEARCH_LIMIT:
            logging.error('count of search is too large.')
            raise ValueError
        try:
            results = self.__api. \
                request('users/search', {'q': keyword, 'page': page, 'count': count, 'include_entities': False})
            if results.status_code >= 400:
                raise TwitterRequestError(results.status_code)
        except TwitterRequestError as err:
            logging.error(err.message)
            raise TwitterRequestError(err.status_code)
        except TwitterConnectionError as err:
            logging.error(err.message)
            raise
        else:
            results_count = len([self.__data_storage.update(
                topic, cl_name, {'account_id': result['account_id']}, {'$set': result}, upsert=True)
                                 for result in (self._load_user_info(data) for data in results)])
            return -1 if results_count < count else page + 1

    @staticmethod
    def __loadUserInfo(data):
    # 学长写的用户定义，老的帮本
    # 当时间为1970.1.1 00:00:00以及更早之前时会报错，将该情况结果定为0
        try:
            created_at = int(time.mktime(time.strptime(data['created_at'], "%a %b %d %H:%M:%S +0000 %Y"))) + 8 * 3600
        except OverflowError:
            created_at = 0
        result = {'account_id': data['id'],
                      'screen_name': data['screen_name'],
                      'name': data['name'],
                      'profile_image_url_https': '400x400'.join(data['profile_image_url_https'].rsplit('normal', 1)),
                      'profile_banner_url':
                          data.get('profile_banner_url') + '/1500x500' if data.get('profile_banner_url') else None,
                      'statuses_count': data['statuses_count'],
                      'favourites_count': data['favourites_count'],
                      'friends_count': data['friends_count'],
                      'followers_count': data['followers_count'],
                      'geo_enabled': data['geo_enabled'],
                      'location': data['location'].strip(),
                      'time_zone': data['time_zone'],
                      'description': data['description'].replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip(),
                      'lang': data['lang'].lower(),
                      'listed_count': data['listed_count'],
                      'protected': data['protected'],
                      'verified': data['verified'],
                      'alive': True,
                      'created_at': created_at,
                      'account_update_time': int(time.time() + time.timezone),
                      'classification':'news'}
        return result


    @staticmethod
    def _load_user_info(data):
        """
        转换Twitter返回的数据为待存储数据
        :param data: Twitter数据
        :return: 转换后数据
        """
        # 当时间为1970.1.1 00:00:00以及更早之前时会报错，将该情况结果定为0
        try:
            created_at = int(time.mktime(time.strptime(data['created_at'], "%a %b %d %H:%M:%S +0000 %Y"))) + 8 * 3600
        except OverflowError:
            created_at = 0
        data['profile_image_url_https'] = '400x400'.join(data['profile_image_url_https'].rsplit('normal', 1))
        data['profile_banner_url'] = data.get('profile_banner_url') + '/1500x500' if data.get('profile_banner_url') else None
        data['alive'] = True
        data['account_update_time'] = int(time.time())
        data['useful'] = True
        data['classification'] = 'think_tank'
        data['created_at_bj'] = created_at
        return data


# 多线程调用API
class MultiProcessAPI(object):
    def __init__(self, data_storage, proxyList=None, token_db=None, debug=False):
        """
        初始化
        :param data_storage: 设置数据库，见DataStorage类
        :param proxyList: 设置代理，格式：['127.0.0.1:8080']
        :param token_db: 设置token数据表，格式：{'host': 'ip:port', 'db': 'database_name', 'cl': 'collection_name'}
        :return: 无
        """
        if not isinstance(data_storage, DataStorage):
            raise TypeError("type of data_storage is wrong.")
        self.__proxyList = proxyList
        self.__data_storage = data_storage
        self.__data_storage.update_collections([token_db or TOKEN_COLLECTION, RECORD_COLLECTION])
        cursor = self.__data_storage.get('token').find({'consumer_key': {'$exists': True}},
                                                       {'consumer_key': 1, 'consumer_secret': 1,
                                                        'access_token': 1, 'access_token_secret': 1, '_id': 0})
        self.__token_list = [data for data in cursor]
        self.debug = debug

    def get_users_lookup(self, process_num=PROCESS_COUNT, user_num=20000, cl_name=None,
                            account_id_list=None, screen_name_list=None):
        """
        批量获取用户详细信息
        :param process_num: 线程数
        :param user_num: 一次采集用户总数
        :param cl_name: collection名
        :param account_id_list: 指定用户id列表
        :param screen_name_list: 指定用户screen_name列表
        当account_id_list和screen_name_list为空时采集数据库中未采集过的用户
        :return: 是否还有用户需要采集
        """
        if not cl_name:
            logging.error('no collection name')
            return False
        logging.debug('start')
        # 从数据库中获取用户ID
        if not account_id_list and not screen_name_list:
            # 获取未更新过的用户
            cursor = self.__data_storage.get(cl_name).find({'alive': None},
                                            {'_id': 0, 'id': 1}).limit(user_num)
            if cursor.count(True) == 0:
                return False
            account_id_list = [user_id['id'] for user_id in cursor]
            if not self.debug:
                pool = Pool(process_num)
                [pool.apply_async(self.__get_users_lookup, (cl_name, data, None,))
                 for data in list_split(account_id_list, num=process_num)]
                pool.close()
                pool.join()
            # debug时只运行单线程
            else:
                [self.__get_users_lookup(cl_name, data, None, )
                 for data in list_split(account_id_list, num=process_num)]
            return True
        else:
            self.__get_users_lookup(cl_name, account_id_list, screen_name_list)
            return False

    def __get_users_lookup(self, cl_name, account_id_list=None, screen_name_list=None):
        """
        单线程获取用户详细信息
        :param cl_name: collection名
        :param account_id_list: 指定用户id列表
        :param screen_name_list: 指定用户screen_name列表
        当account_id_list和screen_name_list为空时采集数据库中未采集过的用户
        :return:
        """
        logging.debug('start')
        # 随机选择token
        token_index = random.randrange(len(self.__token_list))
        api = API(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)], data_storage=self.__data_storage, token=self.__token_list[token_index])
        if not account_id_list:
            account_id_list = []
        if not screen_name_list:
            screen_name_list = []
        if screen_name_list:
            with_screen_name = True
            user_list = account_id_list + screen_name_list
        else:
            with_screen_name = False
            user_list = account_id_list
        # 将待采集列表按长度拆分，再进行探测
        for sub_list in list_split(user_list, length=API_LOOKUP_LIMIT):
            success = False
            while not success:
                try:
                    api.get_users_lookup(user_list=sub_list, with_screen_name=with_screen_name,
                                         cl_name=cl_name)
                except TwitterConnectionError:
                    time.sleep(10)
                except TwitterRequestError as err:
                    # token达到使用上限，需更换
                    if err.status_code == 429 or 403:
                        token_index = random.randrange(len(self.__token_list))
                        api.set_token(self.__token_list[token_index])
                        api.set_proxy(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)])
                        api.set_api()
                    # elif err.status_code == 403:

                    else:
                        pass
                else:
                    success = True

    def get_users_follow(self,process_num=4, user_num=1000, max_list_num=MAX_FOLLOW, user_type='friends',
                         update_cl_name=None, upsert_cl_name=None, account_id_list=None, screen_name_list=None):
        """
        批量获取用户的friends或followers
        :param process_num: 线程数
        :param user_num: 一次采集用户总数
        :param max_list_num: 最多采集follow数
        :param user_type: 采集类型，有'friends'和'followers'两种
        :param update_cl_name: 待更新列表的用户所在collection名
        :param upsert_cl_name: 待插入新用户的collection名
        :param account_id_list: 指定待更新列表的用户id列表
        :param screen_name_list: 指定待更新列表的用户screen_name列表
        :return: 是否还有用户待采集
        """
        if not update_cl_name:
            logging.error('no collection name')
            return False
        logging.debug('start')
        # 从数据库中获取待采集用户ID列表
        if not account_id_list and not screen_name_list:
            cursor = self.__data_storage.get(update_cl_name). \
                find({'captured_' + user_type + '_count': {'$exists': False}, 'protected': False, 'alive': True},
                     {'_id': 0, 'id': 1}).limit(user_num)
            if cursor.count(True) == 0:
                return False
        # 从输入参数中获取待采集用户ID列表
        else:
            if not account_id_list:
                account_id_list = []
            if not screen_name_list:
                screen_name_list = []
            cursor = account_id_list + screen_name_list
            logging.info('list: %s' % str(cursor))
        if not self.debug:
            pool = Pool(process_num)
            [pool.apply_async(self.__get_users_follow,
                              (user_type,
                               data if isinstance(data, (int, long, str, unicode)) else data.get('id'),
                               update_cl_name, upsert_cl_name, max_list_num,)) for data in cursor]
            pool.close()
            pool.join()
        # debug时运行单线程
        else:
            [self.__get_users_follow(user_type,
                                     data if isinstance(data, (int, long, str, unicode)) else data.get('id'),
                                     update_cl_name, upsert_cl_name, max_list_num,) for data in cursor]
        return not account_id_list and not screen_name_list

    def __get_users_follow(self, user_type, user, update_cl_name, upsert_cl_name, max_list_num):
        """
        单线程获取用户的friends或followers
        :param user_type: 采集类型，有'friends'和'followers'两种
        :param user: 待采集用户ID或screen_name
        :param update_cl_name: 待更新列表的用户所在collection名
        :param upsert_cl_name: 待插入新用户的collection名
        :param max_list_num: 最多采集个数
        :return: 无
        """
        logging.debug('start')
        # 随机选择token
        token_index = random.randrange(len(self.__token_list))
        api = API(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)],
                  data_storage=self.__data_storage, token=self.__token_list[token_index])
        search_type = 'id' if isinstance(user, (int, long)) else 'screen_name'
        # 查找临时记录数据库，判断该用户是否已采集了部分列表
        record = self.__data_storage.get('record').find_one({'type': 'api_' + user_type, search_type: user})
        # 若是，则继续采集
        if record:
            next_cursor = record.get('next_cursor')
            rest_num = record.get('rest_num')
        # 若否，则重头采集
        else:
            next_cursor = -1
            rest_num = max_list_num
        total_get_num = 0
        captured_count = max_list_num - rest_num
        get_num = 0
        while True:
            success = False
            # 重试直至成功
            while not success:
                try:
                    next_cursor, rest_num, get_num = \
                        api.get_users_ids(user, user_type, next_cursor, rest_num, update_cl_name, upsert_cl_name)
                except TwitterConnectionError:
                    time.sleep(10)
                except TwitterRequestError as err:
                    # token使用到达上限，需更换
                    if err.status_code == 429:
                        token_index = random.randrange(len(self.__token_list))
                        api.set_token(self.__token_list[token_index])
                        api.set_proxy(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)])
                        api.set_api()
                else:
                    success = True
            total_get_num += get_num
            # 该用户的friends/followers列表已采集完毕
            if next_cursor == 0 or rest_num == 0:
                logging.info('%s finish, next_cursor=%d, rest_num=%d' % (str(user), next_cursor, rest_num))
                self.__data_storage.remove_local('record', {'type': 'api_' + user_type, search_type: user})
                self.__data_storage.update(update_cl_name, {search_type: user},
                                           {'$set': {'captured_' + user_type + '_count': total_get_num + captured_count}})
                logging.info('%s: %s, type: %s, total: %d' % (search_type, str(user), user_type, total_get_num))
                return
            # 该用户的friends/followers列表还未采集完毕
            else:
                logging.info('%s continue, next_cursor=%d, rest_num=%d' % (str(user), next_cursor, rest_num))
                self.__data_storage.update_local('record', {'type': 'api_' + user_type, search_type: user},
                                                 {'$set': {'next_cursor': next_cursor, 'rest_num': rest_num}},
                                                 upsert=True)

    def get_users_picture(self, picture_type='avatar', process_num=8, user_num=2000,
                          cl_name=None, base_path=BASE_PATH, tableName=None):
        """
        批量获取用户的头像或背景条图片
        :param picture_type: 图片类型，有'avatar'和'banner'两种
        :param process_num: 线程数
        :param user_num: 一次采集用户总数
        :param cl_name: collection名
        :param base_path: 图片存储路径
        :return: 是否还有用户需要采集
        """
        if not cl_name:
            logging.error('no collection name')
            return False
        if picture_type not in ['avatar', 'banner']:
            logging.error('%s is not valid picture type.' % picture_type)
            return False
        logging.debug('start')
        # 根据图片类型选择对应的URL字段以及本地路径记录字段
        if picture_type == 'avatar':
            picture_url = 'profile_image_url_https'
            # path_name = 'profile_storage_path'
        else:
            picture_url = 'profile_banner_url'
            # path_name = 'banner_storage_path'
        # 从数据库中获取未采集过指定类型图片的用户ID
        cursor = self.__data_storage.get(cl_name).find(
            {'alive': True, picture_type+ '_captured': None},
            {'_id': 0, 'id': 1, picture_url: 1}).limit(user_num)
        if cursor.count(True) == 0:
            logging.debug('none')
            return False
        if not self.debug:
            pool = Pool(process_num)
            [pool.apply_async(self.__get_users_picture,
                              (picture_type, user, cl_name,tableName,base_path,))
             for user in cursor]
            pool.close()
            pool.join()
        # debug时运行单线程
        else:
            [self.__get_users_picture(picture_type, user, cl_name ,tableName, base_path,)
             for user in cursor]
        return True

    def __get_users_picture(self,picture_type, user, cl_name ,tableName ,base_path=BASE_PATH):
        """
        单线程获取用户的头像或背景条图片
        :param picture_type: 图片类型，有'avatar'和'banner'两种
        :param user: 数据库中提取的一条用户信息
        :param cl_name: collection名
        :param base_path: 图片存储路径
        :return:
        """
        logging.debug('start')
        # 随机选择token
        token = self.__token_list[random.randrange(len(self.__token_list))]
        api = API(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)], data_storage=self.__data_storage, token=token)
        # 根据图片类型选择对应的URL字段以及本地路径记录字段
        if picture_type == 'avatar':
            # path_name = 'profile_storage_path'
            picture_url = 'profile_image_url_https'
        else:
            # path_name = 'banner_storage_path'
            picture_url = 'profile_banner_url'
        # 重试上限为2次
        fail = 2
        while fail:
            try:
                api.get_user_pictures(user.get('id'), user.get(picture_url), cl_name, picture_type, tableName, base_path)
            except requests.HTTPError as err:
                # 当图片不存在
                if err.response.status_code == 404 or err.response.status_code == 500:
                    # 若为第一次访问，则更新用户信息，判断用户是否改变了图片
                    if fail == 2:
                        success = False
                        while not success:
                            try:
                                api.get_users_lookup(user_list=[user.get('id')], cl_name=cl_name)
                            except TwitterConnectionError:
                                time.sleep(10)
                            except TwitterRequestError as err:
                                if err.status_code == 429:
                                    token_index = random.randrange(len(self.__token_list))
                                    api.set_token(self.__token_list[token_index])
                                    api.set_proxy(proxy=self.__proxyList[random.randint(0,len(self.__proxyList)-1)])
                                    api.set_api()
                                else:
                                    pass
                            # 重新获取图片的URL
                            else:
                                success = True
                                fail -= 1
                                user = self.__data_storage.get(cl_name).find_one(
                                    {'id': user.get('id')}, {'_id': 0, 'id': 1, picture_url: 1})
                    # 若第二次访问图片仍不存在，则认为图片失效
                    else:
                        self.__data_storage.update(cl_name,{'id': user.get('id')},{'$set':{picture_type + '_captured':True}})
                        fail -= 1
            except requests.RequestException:
                time.sleep(10)
            # 若成功访问到图片则跳出循环
            else:
                fail = 0
