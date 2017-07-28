# -*- coding:utf-8 -*-
import functools
import os
import time
import logging
from logging.handlers import RotatingFileHandler
import sys
from pymongo import MongoClient
# from setproctitle import setproctitle
import psutil


# def status_collection(with_log=False, mongo=None):
#     """
#     在数据库中记录进程
#     :param with_log:
#     :param mongo:
#     :return:
#     """
#     def status_collection_decorator(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             status = get_status(func)
#             parameters = {'args': args, 'kwargs': kwargs}
#             status_mongo = StatusMongo(mongo)
#             status_mongo.save_status(status, parameters)
#             if with_log:
#                 log_name = status.get('log_name')
#                 result = set_logging(log_name)(func)(*args, **kwargs)
#             else:
#                 result = func(*args, **kwargs)
#             status_mongo.set_running_state(status.get('log_name'), 'finish')
#             return result
#         return wrapper
#     return status_collection_decorator


def set_logging():
    def set_logging_decorator(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            log_name = get_status(func).get("log_name")
            init_logging(log_name)
            return func(*args, **kwargs)
        return _wrapper
    return set_logging_decorator


def sub_process(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            child_pid = os.fork()
            if child_pid == 0:
                os.chdir("/")
                os.setsid()
                grandchild_pid = os.fork()
                if grandchild_pid == 0:
                    sys.stdout.flush()
                    sys.stderr.flush()
                    file_null = file(os.devnull, 'w+')
                    # 重定向标准输入/输出/错误
                    os.dup2(file_null.fileno(), sys.stdout.fileno())
                    os.dup2(file_null.fileno(), sys.stderr.fileno())
                    func(*args, **kwargs)
                    os._exit(0)
                else:
                    print 'grandchild pid: %d' % grandchild_pid
                    time.sleep(0.1)
                    os._exit(0)
            else:
                print 'main pid: %d' % os.getpid()
                print 'child pid: %d' % child_pid
                os.waitpid(child_pid, 0)
                time.sleep(0.1)
                exit(0)
        except OSError:
            raise
    return wrapper


def init_logging(log_name):
    """
    logging初始化设置（在main开始处执行）
    :param log_name: log文件名
    """
    formatter = logging.Formatter(fmt='%(asctime)s %(module)s %(funcName)s %(thread)d %(threadName)s '
                                  '[line:%(lineno)d] %(levelname)s %(message)s',
                                  datefmt='%a %d %b %Y %H:%M:%S')
    file_logging = RotatingFileHandler(
        'D:/YjProject/yj_project/log/'+log_name+'.log', maxBytes=10*1024*1024, backupCount=5)
    file_logging.setLevel(logging.INFO)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    file_logging.setFormatter(formatter)
    console.setFormatter(formatter)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(console)
    logging.getLogger().addHandler(file_logging)


def get_status(func):
    func_name = func.__name__
    # setproctitle(func_name)
    pid = os.getpid()
    process = psutil.Process(pid)
    start_time = int(process.create_time())
    print time.ctime(start_time)
    log_name = '%s_%d_%d' % (func_name, start_time, pid)
    result = {'start_time': start_time,
              'start_time_str': time.ctime(start_time),
              'process_name': func_name,
              'pid': pid,
              'log_name': log_name}
    return result


class StatusMongo:
    mongo_default = {
        # 'host': '127.0.0.1',
        'host': 'mongodb://mongo:123456@222.197.180.150',
        'db': 'condition_monitor',
        'cl': 'process',
    }

    def __init__(self, mongo=None):
        mongo = mongo or self.mongo_default
        self.collection = MongoClient(mongo.get('host')).get_database(mongo.get('db')).get_collection(mongo.get('cl'))

    def save_status(self, status, parameters):
        if 'pid' in status:
            end_time = int(time.time())
            self.collection.update(
                {'pid': status.get('pid'), 'running_state': 'running'},
                {'$set': {'running_state': 'terminate', 'end_time': end_time, 'end_time_str': time.ctime(end_time)}},
                multi=True)
        self.collection.update(status, {'$set': {'running_state': 'running', 'parameters': parameters}}, upsert=True)

    def set_running_state(self, log_name, running_state):
        end_time = int(time.time())
        self.collection.update_one({'log_name': log_name},
                                   {'$set': {'running_state': running_state,
                                             'end_time': end_time,
                                             'end_time_str': time.ctime(end_time)}})

    def find_all(self):
        return self.collection.find()

print time.gmtime(0)