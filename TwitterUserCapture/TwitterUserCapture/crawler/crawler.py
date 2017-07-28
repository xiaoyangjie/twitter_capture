# -*- coding: UTF-8 -*-
# python2.7
import urllib
import urllib2
from httplib import HTTPException
import socks
from sockshandler import SocksiPyHandler
import cookielib
from bs4 import BeautifulSoup
import json
import re
import time
from multiprocessing.dummy import Pool
import logging
import os
from .crawler_error import CrawlerRequestError, CrawlerConnectionError, CrawlerEmptyException, CrawlerError
from ..data_storage import DataStorage
from ..constants import *


class Crawler(object):
    """
    爬虫采集Twitter用户信息
    """
    def __init__(self, proxy=PROXY, data_storage=None, timeout=None):
        """
        初始化
        :param proxy:代理，类型：str，格式：'host:port'
        :param data_storage:数据库和单导的连接，类型：DataStorage
        :param timeout:超时，类型：int，单位：秒
        :return: None
        """
        # 创建/连接数据库
        self.__data_storage = data_storage or DataStorage()
        self.__data_storage.update_collections([COOKIES_COLLECTION, RECORD_COLLECTION, ID_COLLECTION])

        self.__opener = None
        self.__timeout = timeout
        self.__head = HTTP_HEADER
        self.set_cookie(first=True)
        self.set_proxy(proxy)

    def set_proxy(self, proxy):
        """
        设置代理
        :param proxy: 代理，类型：str，格式：'host:port'
        :return: None
        """
        if not proxy:
            self.__opener = urllib2.build_opener()
        else:
            proxy_handler = SocksiPyHandler(socks.SOCKS5, proxy.split(':')[0], int(proxy.split(':')[1]))
            self.__opener = urllib2.build_opener(proxy_handler)
        self.__opener.addheaders = zip(self.__head.keys(), self.__head.values())

    def set_cookie(self, first=False):
        """
        设置cookie
        :param first: 是否第一次设置cookie，类型：bool
        :return: None
        """
        cursor = self.__data_storage.get('cookies').find().sort('used_time').limit(1)
        try:
            data = cursor.next()
        except StopIteration:
            return None
        data['used_time'] = int(time.time() + time.timezone)
        cookie_head = {'cookie': data.get('cookie', '')}
        self.__head.update(cookie_head)
        if not first:
            self.__opener.addheaders = zip(self.__head.keys(), self.__head.values())
            self.__data_storage.get('cookies').save(data)

    def net_test(self):
        """
        网络连接测试
        :return: None
        """
        url_test = 'https://twitter.com/bbc/following'
        try:
            self.open_url(url_test)
        except CrawlerConnectionError as err:
            logging.error('can not connect the url, net test fail.')
            raise CrawlerConnectionError(err)

    def login(self, user_name, pass_word):
        """
        登录，获取cookie
        :param user_name: 用户登录名，类型：str
        :param pass_word: 密码，类型：str
        :return: 是否成功，类型：bool
        """
        url_twitter_login = 'https://twitter.com/sessions'
        try:
            resp = self.open_url(url_twitter_login)
        except CrawlerError:
            logging.error('Please login again: %s' % user_name)
            return False
        _data = resp.decode('utf-8', 'ignore')
        authenticity_token = re.search(r'name=\"authenticity_token\" value=\"[a-zA-Z0-9]+\"', _data)
        authenticity_token = authenticity_token.group(0).split('\"')[3]
        login_data = urllib.urlencode({'session[username_or_email]': user_name,
                                       'session[password]': pass_word,
                                       'authenticity_token': authenticity_token}).encode('utf-8')
        try:
            self.open_url(url_twitter_login, login_data)
        except CrawlerError:
            logging.error('Please login again: %s' % user_name)
            return False
        cookie_data = ';'.join(['%s=%s' % (cookie.name, cookie.value) for cookie in cookielib.CookieJar()])
        self.__data_storage.get('cookies').save({'account_id': user_name, 'cookie': cookie_data})
        logging.info('login finished')
        return True

    def open_url(self, url, post_data=None):
        """
        提交HTTP请求，获取响应信息
        :param url: 目标URL，类型：str
        :param post_data: POST数据（GET时为None）
        :return: html数据
        """
        resp = None
        html = None
        try:
            if self.__timeout:
                resp = self.__opener.open(url, post_data, timeout=self.__timeout)
            else:
                resp = self.__opener.open(url, post_data)
            html = resp.read()
        except urllib2.URLError as error:
            if hasattr(error, 'code'):
                print 'The server couldn\'t fulfill the request.'
                if error.code == 404:
                    logging.info('Error code: 404')
                else:
                    logging.exception('Error code: %d' % error.code)
                raise CrawlerRequestError(error.code)
            if hasattr(error, 'reason'):
                print 'We failed to reach a server.'
                logging.exception('Error reason: %s' % error.reason)
                if error.reason.errno:
                    raise CrawlerConnectionError(error.reason.errno)
                else:
                    raise CrawlerConnectionError(error.reason)
        except HTTPException:
            logging.exception('HTTPException')
            raise CrawlerConnectionError('HTTPException')
        except socks.ProxyConnectionError as error:
            logging.exception(error.msg)
            raise CrawlerConnectionError(error.msg)
        except:
            logging.exception('Other')
            raise CrawlerConnectionError('Error')
        finally:
            if resp:
                resp.close()
        if html:
            return html
        else:
            raise CrawlerEmptyException

    def get_screen_name_by_id(self, cl_name, process_num=PROCESS_COUNT):
        """
        通过account_id获取screen_name（多线程）
        :param cl_name: 数据表名，类型：str
        :param process_num: 线程数，类型：int
        :return: 是否有数据需要采集，类型：bool
        """
        logging.info('start')
        cursor = self.__data_storage.get(cl_name).find({'alive': True, 'screen_name': ''}, {'account_id': 1})
        if cursor.count(True) == 0:
            return False
        pool = Pool(process_num)
        results = [pool.apply_async(self.__get_screen_name_by_id, (data['account_id'], cl_name,)) for data in cursor]
        pool.close()
        while True:
            print 'alive'
            # self.heartbeat.send_heartbeat()
            time.sleep(10)
            ready = 0
            for result in (result for result in results if result.ready()):
                try:
                    result.get()
                except CrawlerRequestError as err:
                    if err.status_code != 404:
                        pool.terminate()
                        raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    pool.terminate()
                    raise
                ready += 1
            if ready == len(results):
                return True

    def __get_screen_name_by_id(self, account_id, cl_name):
        """
        通过account_id获取screen_name（单线程）
        :param account_id: 用户ID，类型：int
        :param cl_name: 数据表名，类型：str
        :return: 用户screen_name，类型：str
        """
        logging.info('start get screen_name by id: %d' % account_id)
        intent_page = None
        for i in range(3):
            # 通过ID访问用户简易主页
            try:
                intent_page = self.open_url('https://twitter.com/intent/user?user_id=%d' % account_id)
            except CrawlerRequestError as err:
                if err.status_code == 404:
                    self.__data_storage. \
                        update(cl_name, {'account_id': account_id},
                               {'$set': {'alive': False, 'account_update_time': int(time.time() + time.timezone)}})
                    self.__data_storage.remove_local('record', {'account_id': account_id})
                raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i < 2:
                    logging.warning('Fail to open the page of user id: %d, retry.' % account_id)
                else:
                    raise
            else:
                break
        intent_page_data = BeautifulSoup(intent_page, 'html.parser')
        try:
            user_screen_name = intent_page_data. \
                find('form', class_=re.compile(r"follow")).find('input', {'name': "screen_name"}).get("value")
        except AttributeError:
            user_screen_name = None
        if not user_screen_name:
            self.__data_storage. \
                update(cl_name, {'account_id': account_id},
                       {'$set': {'alive': False, 'account_update_time': int(time.time() + time.timezone)}})
            self.__data_storage.remove_local('record', {'account_id': account_id})
            raise CrawlerEmptyException
        else:
            self.__data_storage.update(cl_name, {'account_id': account_id}, {'$set': {'screen_name': user_screen_name}})
            self.__data_storage.update_local('record', {'account_id': account_id},
                                             {'$set': {'screen_name': user_screen_name}}, multi=True)
            success_str = 'get %d\'s screen_name: %s' % (account_id, user_screen_name)
            logging.info(success_str)
            return user_screen_name

    def get_users_lookup(self, user_num, cl_name, process_num=PROCESS_COUNT):
        """
        获取用户详细信息（多线程）
        :param user_num: 批量获取的用户数，类型：int
        :param cl_name: 数据表名，类型：str
        :param process_num: 线程数，类型：int
        :return: 是否有数据需要采集，类型：bool
        """
        logging.info('start get users lookup.')
        cursor = self.__data_storage.get(cl_name).find({'alive': None},
                                                       {'screen_name': 1, 'account_id': 1, '_id': 0}).limit(user_num)
        if cursor.count(True) == 0:
            return False

        pool = Pool(process_num)
        results = [pool.apply_async(self.__get_user_lookup, (data, cl_name,)) for data in cursor]
        pool.close()
        while True:
            print 'alive'
            # self.heartbeat.send_heartbeat()
            time.sleep(10)
            ready = 0
            for result in (result for result in results if result.ready()):
                try:
                    result.get()
                except CrawlerRequestError as err:
                    if err.status_code != 404:
                        pool.terminate()
                        raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    pool.terminate()
                    raise
                ready += 1
            if ready == len(results):
                return True

    def __get_user_lookup(self, user_data, cl_name):
        """
        获取用户详细信息（单线程）
        :param user_data: 用户信息，类型：dict，至少包含'account_id'或'screen_name'中的一个字段
        :param cl_name: 数据表名，类型：str
        :return: None
        """
        user_screen_name = user_data.get('screen_name', '')
        if not user_screen_name:
            try:
                user_screen_name = self.__get_screen_name_by_id(user_data['account_id'], cl_name)
            except CrawlerError:
                raise
        logging.info('Begin to get %s\'s lookup.' % user_screen_name)
        user_home_page = None
        for i in range(3):
            try:
                user_home_page = self.open_url('https://twitter.com/%s' % user_screen_name)
            except CrawlerRequestError as err:
                if err.status_code == 404 or i == 2:
                    self.__data_storage.\
                        update(cl_name, {'screen_name': user_screen_name}, {'$set': {'screen_name': ''}})
                raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i < 2:
                    logging.warning('Fail to open the page of %s, retry.' % user_screen_name)
                else:
                    self.__data_storage.\
                        update(cl_name, {'screen_name': user_screen_name}, {'$set': {'screen_name': ''}})
                    raise
            else:
                break
        user_home_page_data = BeautifulSoup(user_home_page, 'html.parser')
        json_data = user_home_page_data.find('input', class_="json-data")
        if json_data:
            filled_data = self._load_profile_from_json_data(json_data)
            account_id = filled_data.get('account_id', user_data.get('account_id', None))
            if not account_id:
                return
            self.__data_storage. \
                update(cl_name, {'account_id': account_id}, {'$set': filled_data}, upsert=True)
            if not filled_data['alive'] or filled_data['protected']:
                self.__data_storage.remove_local('record', {'account_id': filled_data['account_id']})
        logging.info('Getting %s\'s lookup is finished.' % user_screen_name)
        return

    def get_users_picture(self, user_num, cl_name, process_num=PROCESS_COUNT, picture_type='avatar', picture_path=None):
        """
        下载用户图片（多线程）
        :param user_num: 批量下载图片数，类型int
        :param cl_name: 数据表名，类型：str
        :param process_num: 线程数，类型：int
        :param picture_type: 图片种类，类型：str，目前有'avatar'和'banner'两种
        :param picture_path: 本地存放路径，类型str
        :return: 是否有图片需要下载，类型：bool
        """
        logging.info('start get %s' % picture_type)
        # 获取前num个未更新过的数据
        if picture_type == 'avatar':
            cursor = self.__data_storage.get(cl_name). \
                find({'alive': True, 'profile_storage_path': None, 'profile_image_url_https': {'$exists': True}},
                     {'account_id': 1, 'profile_image_url_https': 1, '_id': 0}).limit(user_num)
            picture_path = picture_path or '/home/server-cj/twitter/profile_img/'
        elif picture_type == 'banner':
            cursor = self.__data_storage.get(cl_name). \
                find({'alive': True, 'banner_storage_path': None, 'profile_banner_url': {'$exists': True}},
                     {'account_id': 1, 'profile_banner_url': 1, '_id': 0}).limit(user_num)
            picture_path = picture_path or '/home/server-cj/twitter/banner/'
        else:
            logging.warning('unsupported type: %s' % picture_type)
            raise ValueError('unsupported type: %s' % picture_type)
        if cursor.count(True) == 0:
            return False
        # 开4个线程池
        pool = Pool(process_num)
        results = \
            [pool.apply_async(self.__get_user_picture, (data, cl_name, picture_type, picture_path,)) for data in cursor]
        pool.close()
        while True:
            print 'alive'
            # self.heartbeat.send_heartbeat()
            time.sleep(10)
            ready = 0
            for result in (result for result in results if result.ready()):
                try:
                    result.get()
                except CrawlerRequestError as err:
                    if err.status_code != 404:
                        pool.terminate()
                        raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    pool.terminate()
                    raise
                ready += 1
            if ready == len(results):
                return True

    # 将用户头像保存至本地
    def __get_user_picture(self, user, cl_name, picture_type, picture_path):
        """
        下载用户图片（单线程）
        :param user: 用户信息，类型：dict
        :param cl_name: 数据表名，类型：str
        :param picture_type: 图片种类，类型：str，目前有'avatar'和'banner'两种
        :param picture_path: 本地存放路径，类型str
        :return: None
        """
        account_id = user['account_id']

        if picture_type == 'avatar':
            url = user.get('profile_image_url_https')
            path_name = 'profile_storage_path'
        elif picture_type == 'banner':
            url = user.get('profile_banner_url')
            path_name = 'banner_storage_path'
        else:
            logging.warning('unsupported type: %s' % picture_type)
            return
        if not url:
            logging.warning('empty url')
            self.__data_storage. \
                update(cl_name, {'account_id': account_id}, {'$set': {path_name: ''}})
            return
        logging.info('start get %s, url: %s' % (picture_type, url))
        daily_dir_path = time.strftime("%Y-%m-%d", time.gmtime()) + '/'
        if picture_type == 'avatar':
            file_name = str(account_id) + url.split('400x400')[-1]
        else:
            file_name = str(account_id) + '.jpg'
        if not os.path.exists(picture_path + daily_dir_path):
            try:
                os.mkdir(picture_path + daily_dir_path)
            except os.error:
                pass
        whole_path = picture_path + daily_dir_path + file_name
        image = None
        not_found_time = 0
        for i in range(3):
            try:
                image = self.open_url(url)
            except CrawlerRequestError as err:
                if err.status_code == 404:
                    not_found_time += 1
                    if not_found_time > 1:
                        self.__data_storage. \
                            update(cl_name, {'account_id': account_id}, {'$set': {path_name: ''}})
                        raise CrawlerRequestError(err.status_code)
                    try:
                        self.__get_user_lookup(user, cl_name)
                    except CrawlerError:
                        raise
                    else:
                        continue
                else:
                    raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i == 2:
                    raise
            else:
                break
        image_file = open(whole_path, 'wb')
        image_file.write(image)
        image_file.close()
        self.__data_storage.update(cl_name, {'account_id': account_id},
                                   {'$set': {path_name: daily_dir_path + file_name}})
        logging.info('got avatar at: %s' % whole_path)

    def get_users_follow(self, user_num, update_cl_name, upsert_cl_name=None, process_num=PROCESS_COUNT,
                         follow_type="friends", follow_num=INFINITE):
        """
        获取用户的friends/followers列表（多线程）
        :param user_num: 一次最多处理用户数，类型：int
        :param update_cl_name: 用于更新列表的collection名，类型：str
        :param upsert_cl_name: 用于插入新用户的collection名，类型：str
        :param process_num: 线程数，类型：int
        :param follow_type: 要采集用户的种类，类型：str，有"friends"和"followers"两种
        :param follow_num: 采集上限，类型：int
        :return: None
        """
        logging.info('start')
        users_list = []
        upsert_cl_name = upsert_cl_name or update_cl_name
        cursor = self.__data_storage.get('record').\
            find({'type': 'crawler_'+follow_type, 'user_min_pos': {'$exists': True}},
                 {'_id': 0, 'screen_name': 1, 'account_id': 1, 'user_min_pos': 1, 'rest_users_num': 1}).\
            limit(user_num)
        cursor_count = cursor.count(True)
        users_list += [data for data in cursor]
        if cursor_count < user_num:
            cursor = self.__data_storage.get(update_cl_name).\
                find({'last_update_' + follow_type: {'$exists': False}, 'screen_name': {'$exists': True, '$ne': ''}},
                     {'_id': 0, 'screen_name': 1, 'account_id': 1}).limit(user_num - cursor_count)
            cursor_count += cursor.count(True)
            users_list += [data for data in cursor]
            if cursor_count == 0:
                user_screen_name = raw_input('Please input a screen name: ')
                if not user_screen_name:
                    return 'finish'
                user = {'screen_name': user_screen_name}
                try:
                    self.__get_user_lookup(user, update_cl_name)
                except CrawlerError:
                    raise
                else:
                    return
        pool = Pool(process_num)
        results = [pool.apply_async(self.__get_user_follow,
                                    (data, follow_type, update_cl_name, upsert_cl_name, follow_num,))
                   for data in users_list]
        pool.close()
        while True:
            print 'alive'
            # self.heartbeat.send_heartbeat()
            time.sleep(10)
            ready = 0
            for result in (result for result in results if result.ready()):
                try:
                    result.get()
                except CrawlerRequestError as err:
                    if err.status_code != 404:
                        pool.terminate()
                        raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    pool.terminate()
                    raise
                ready += 1
            if ready == len(results):
                return

    # 寻找用户的关注者/被关注者的screen name和ID
    def __get_user_follow(self, user, follow_type, update_cl_name, upsert_cl_name, max_num):
        """
        获取用户的friends/followers列表（单线程）
        :param user: 用户信息，类型：dict
        :param follow_type: 要采集用户的种类，类型：str，有"friends"和"followers"两种
        :param update_cl_name: 用于更新列表的collection名，类型：str
        :param upsert_cl_name: 用于插入新用户的collection名，类型：str
        :param max_num: 采集上限，类型：int
        :return: None
        """
        start_info = 'start getting %s\'s %s.' % (user['screen_name'], follow_type)
        end_info = 'Downloading %s\'s %s ids finish.' % (user['screen_name'], follow_type)
        empty_info = '%s doesn\'t have %s.' % (user['screen_name'], follow_type)
        logging.info(start_info)
        url_type = follow_type
        if url_type == 'friends':
            url_type = 'following'
        rest_users_num = user.get('rest_users_num', INFINITE)
        if rest_users_num == INFINITE or max_num == INFINITE:
            rest_users_num = max_num
        min_pos = abs(user.get('user_min_pos', 0))
        if not min_pos:
            # 用户页面HTML解析
            _data = None
            for i in range(3):
                try:
                    print 'https://twitter.com/%s/%s' % (user['screen_name'], url_type)
                    _data = self.open_url('https://twitter.com/%s/%s' % (user['screen_name'], url_type))
                except CrawlerRequestError as err:
                    if err.status_code == 404:
                        try:
                            self.__get_screen_name_by_id(user['account_id'], update_cl_name)
                        except CrawlerError:
                            raise
                    raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    if i < 2:
                        logging.warning('Fail to open %s page of %s, retry.' % (follow_type, user['screen_name']))
                        continue
                    else:
                        raise
                else:
                    break
            soup = BeautifulSoup(_data, 'html.parser')
            json_data = soup.find('input', class_="json-data")
            if json_data:
                filled_data = self._load_profile_from_json_data(json_data)
                self.__data_storage.update(update_cl_name, {'account_id': user['account_id']}, {'$set': filled_data})
                if not filled_data['alive'] or filled_data['protected']:
                    logging.\
                        info('%s is protected or suspended, can\'t get %s ids.' % (user['screen_name'], follow_type))
                    self.__data_storage.update(update_cl_name, {'account_id': user['account_id']},
                                               {'$addToSet': {follow_type + '_ids': {'$each': []}}})
                    self.__data_storage.remove_local('record', {'account_id': user['account_id']})
                    return
            profiles = soup.find_all('div', class_='ProfileCard  js-actionable-user')
            if not profiles:
                self.__data_storage.update(update_cl_name, {'account_id': user['account_id']},
                                           {'$addToSet': {follow_type + '_ids': {'$each': []}}})
                logging.info(empty_info)
                return
            follow_ids = [int(profile.get('data-user-id')) for profile in profiles]
            rest_users_num -= len(follow_ids) if max_num != INFINITE else 0
            if upsert_cl_name:
                self.__data_storage.\
                    insert(upsert_cl_name, [{'account_id': account_id} for account_id in follow_ids],
                           continue_on_error=True)
            self.__data_storage.update(update_cl_name, {'account_id': user['account_id']},
                                       {'$addToSet': {follow_type + '_ids': {'$each': follow_ids}}})
            if max_num != INFINITE and rest_users_num <= 0:
                logging.info(end_info)
                return
            min_pos = soup.find('div', class_="GridTimeline-items")['data-min-position']

        # 用户页面json解析
        while int(min_pos) > 0 and (max_num == INFINITE or max_num != INFINITE and rest_users_num > 0):
            url_next = 'https://twitter.com/%s/%s/' \
                       'users?include_available_features=1&include_entities=1&' \
                       'max_position=%d&reset_error_state=false' % (user['screen_name'], url_type, int(min_pos))
            _data = None
            self.__data_storage. \
                update_local('record', {'type': 'crawler_'+follow_type, 'account_id': user['account_id']},
                             {'$set': {'screen_name': user['screen_name'],
                                       'user_min_pos': int(min_pos),
                                       'rest_users_num': rest_users_num}}, upsert=True)
            for i in range(3):
                try:
                    _data = self.open_url(url_next)
                except CrawlerRequestError as err:
                    if err.status_code == 404:
                        try:
                            self.__get_user_lookup(user, update_cl_name)
                        except CrawlerError:
                            raise
                    raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    if i < 2:
                        logging.warning('Fail to open next user page: %s, retry.' % str(min_pos))
                        continue
                    else:
                        raise
                else:
                    break
            _data = _data.decode('utf-8')
            try:
                json_data = json.loads(_data)
            except ValueError:
                logging.warning('data from %s is incomplete, retry' % str(min_pos))
                continue
            min_pos = json_data.get('min_position')
            html_data = json_data.get('items_html')
            soup = BeautifulSoup(html_data, 'html.parser')
            profiles = soup.find_all('div', class_='ProfileCard  js-actionable-user')
            if not profiles:
                logging.warning('No user profile: %d, ignore' % min_pos)
                continue
            follow_ids = [int(profile.get('data-user-id')) for profile in profiles]
            rest_users_num -= len(follow_ids) if max_num != INFINITE else 0
            self.__data_storage.insert(upsert_cl_name, [{'account_id': account_id} for account_id in follow_ids],
                                       continue_on_error=True)
            self.__data_storage.update(update_cl_name, {'account_id': user['account_id']},
                                       {'$addToSet': {follow_type + '_ids': {'$each': follow_ids}}})
            logging.info('next')
        logging.info(end_info)
        self.__data_storage.update(update_cl_name, {'account_id': user['account_id']},
                                   {'$set': {'last_update_'+follow_type: int(time.time()+time.timezone)}})
        return

    def get_basic_info_by_incremental_id(self, user_num, cl_name, process_num=8):
        """
        遍历每一个id，更新存在的用户的screen_name
        :param user_num:
        :param cl_name:
        :param process_num:
        :return:
        """
        logging.info('start')
        cursor = self.__data_storage.get('id').find({'next_account_id': {'$exists': True}}).limit(1)
        next_account_id = (cursor.next())['next_account_id']
        pool = Pool(process_num)
        results = [pool.apply_async(self.__get_basic_info_by_incremental_id, (account_id, cl_name,))
                   for account_id in range(next_account_id, next_account_id + user_num)]
        pool.close()
        while True:
            print 'alive'
            # self.heartbeat.send_heartbeat()
            time.sleep(10)
            ready = 0
            for result in (result for result in results if result.ready()):
                try:
                    result.get()
                except CrawlerRequestError as err:
                    if err.status_code != 404:
                        pool.terminate()
                        cursor = self.__data_storage.get('id').find().sort('account_id', -1).limit(1)
                        max_alive_account_id = (cursor.next())['account_id']
                        if max_alive_account_id > next_account_id:
                            self.__data_storage. \
                                update_local('id', {'next_account_id': {'$exists': True}},
                                             {'$set': {'next_account_id': max_alive_account_id - process_num + 1}})
                        raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    pool.terminate()
                    raise
                ready += 1
            if ready == len(results):
                self.__data_storage.update_local(
                    'id', {'next_account_id': {'$exists': True}}, {'$inc': {'next_account_id': user_num}})
                return

    def __get_basic_info_by_incremental_id(self, account_id, cl_name):
        logging.info('start')
        intent_page = None
        for i in range(3):
            # 通过ID访问用户简易主页
            try:
                intent_page = self.open_url('https://twitter.com/intent/user?user_id=%d' % account_id)
            except CrawlerRequestError as err:
                if err.status_code == 404:
                    return
                else:
                    raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i < 2:
                    logging.warning('Fail to open the page of user id: %d, retry.' % account_id)
                    continue
                else:
                    raise
            else:
                break
        intent_page_data = BeautifulSoup(intent_page, 'html.parser')
        user_screen_name = intent_page_data.find('form', class_=re.compile(r"follow"))
        if user_screen_name:
            user_screen_name = user_screen_name.find('input', {'name': "screen_name"}).get("value")
            if user_screen_name:
                self.__data_storage.insert(cl_name, {'account_id': account_id, 'screen_name': user_screen_name})
                self.__data_storage.insert_local(
                    'id', {'account_id': account_id, 'time': int(time.time() + time.timezone)})
                logging.info('get account_id: %d, screen_name: %s' % (account_id, user_screen_name))
                return

    # 根据关键词搜索用户
    def get_account_by_search(self, keyword, cl_name, rest_users_num=300):
        url = 'https://twitter.com/search?f=users&vertical=default&q=%s&src=typd' % keyword
        start_info = 'start getting accounts about %s.' % keyword
        end_info = 'Downloading accounts about %s finish.' % keyword
        logging.info(start_info)
        # 用户页面HTML解析
        _data = None
        for i in range(3):
            try:
                _data = self.open_url(url)
            except CrawlerRequestError as err:
                raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i < 2:
                    logging.warning('Fail to open search page about %s, retry.' % keyword)
                    continue
                else:
                    raise
            else:
                break
        soup = BeautifulSoup(_data, 'html.parser')
        profiles = soup.find_all('div', class_='ProfileCard  js-actionable-user')
        for profile in profiles:
            if rest_users_num <= 0:
                logging.info(end_info)
                return
            screen_name = profile.get('data-screen-name')
            user_id = int(profile.get('data-user-id'))
            user_data = {'account_id': user_id, 'screen_name': screen_name, 'classification': keyword}
            rest_users_num -= 1
            self.__data_storage.insert(cl_name, user_data)
        min_pos = soup.find('div', class_="GridTimeline-items")['data-min-position']
        has_more = True
        # 用户页面json解析
        while has_more and rest_users_num > 0:
            url_next = 'https://twitter.com/i/search/timeline?f=users&vertical=default&q=%s&' \
                       'src=typd&include_available_features=1&include_entities=1&max_position=%s&' \
                       'oldest_unread_id=0&reset_error_state=false' % (keyword, min_pos)
            _data = None
            for i in range(3):
                try:
                    _data = self.open_url(url_next)
                except CrawlerRequestError as err:
                    raise CrawlerRequestError(err.status_code)
                except CrawlerError:
                    if i < 2:
                        logging.warning('Fail to open next user page: %s, retry.' % min_pos)
                        continue
                    else:
                        raise
                else:
                    break
            _data = _data.decode('utf-8')
            try:
                json_data = json.loads(_data)
            except ValueError:
                logging.warning('data from %s is incomplete, retry.' % min_pos)
                continue
            min_pos = json_data.get('min_position')
            has_more = json_data.get('has_more_items')
            html_data = json_data.get('items_html')
            soup = BeautifulSoup(html_data, 'html.parser')
            profiles = soup.find_all('div', class_='ProfileCard  js-actionable-user')
            if not profiles:
                logging.warning('No user profile: %s, ignore' % min_pos)
                continue
            for profile in profiles:
                if rest_users_num <= 0:
                    break
                screen_name = profile.get('data-screen-name')
                user_id = int(profile.get('data-user-id'))
                user_data = {'account_id': user_id, 'screen_name': screen_name, 'classification': keyword}
                rest_users_num -= 1
                self.__data_storage.insert(cl_name, user_data)
            logging.info('next')
        logging.info(end_info)
        return

    # 根据screen_name获取id并存入数据库
    def get_id_by_screen_name(self, screen_name, classification, cl_name):
        if self.__data_storage.get(cl_name).count({'screen_name': screen_name}) > 0:
            return True
        intent_page = None
        for i in range(3):
            try:
                intent_page = self.open_url('https://twitter.com/intent/user?screen_name=%s' % screen_name)
            except CrawlerRequestError as err:
                if err.status_code == 404:
                    return False
                raise CrawlerRequestError(err.status_code)
            except CrawlerError:
                if i < 2:
                    print 'Error: Fail to open the page of user id: %s, retry.' % screen_name
                else:
                    raise
            else:
                break
        intent_page_data = BeautifulSoup(intent_page, 'html.parser')
        user_id = intent_page_data.find('input', {'name': 'profile_id'})
        if not user_id:
            raise CrawlerEmptyException
        user_id = user_id.get('value')
        self.__data_storage.\
            insert(cl_name, {'account_id': int(user_id), 'screen_name': screen_name, 'classification': classification})
        return True

    @staticmethod
    def _load_profile_from_json_data(json_data):
        json_data = json_data.get('value')
        json_data = json.loads(json_data)
        filled_data = None
        data = json_data.get('profile_user')
        if data:
            filled_data = \
                {'account_id': data['id'],
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
                 'description':
                     data['description'].replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip(),
                 'lang': data['lang'].lower(),
                 'listed_count': data['listed_count'],
                 'protected': data['protected'],
                 'verified': data['verified'],
                 'alive': True,
                 'created_at': int(time.mktime(time.strptime(data['created_at'], "%a %b %d %H:%M:%S +0000 %Y")))
                 }
        elif json_data.get('sectionName') == 'suspended':
            filled_data = {'alive': False}
        filled_data['account_update_time'] = int(time.time() + time.timezone)
        return filled_data
