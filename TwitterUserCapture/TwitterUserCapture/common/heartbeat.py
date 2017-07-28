#!/usr/bin/env python  
# -*- coding:UTF-8 -*-  

import string
import psutil
import re
import time
import setproctitle


class Heartbeat(object):
    def __init__(self, process_name, task_type, host_ip, producer):
        self.__process_name = process_name
        setproctitle.setproctitle(process_name)
        self.__task_type = task_type
        self.__host_ip = host_ip

        self.__pid = self.__get_pid(self.__process_name)
        self.__process = psutil.Process(self.__pid)
        self.__producer = producer

    @staticmethod
    def __get_cpu_state(process):
        return process.cpu_percent(interval=3)

    @staticmethod
    def __get_mem_state(process):
        return process.memory_percent()

    @staticmethod
    def __get_pid(name):
        process_list = list(psutil.process_iter())
        regex = "pid=(\d+),\sname=\'%s\'" % name
        pid = 0
        for result in (re.compile(regex).search(str(line)) for line in process_list):
            if result:
                pid = string.atoi(result.group(1))
                print result.group()
                break
        return pid

    def __get_process_state(self):
        process_state = {'node_task_name': self.__process_name,
                         'node_task_type': self.__task_type,
                         'last_status_update_time': time.time(),
                         'cpu_usage': self.__get_cpu_state(self.__process),
                         'memory_usage': self.__get_mem_state(self.__process),
                         'hostIP': self.__host_ip}
        return process_state

    def send_heartbeat(self):
        process_state = self.__get_process_state()
        self.__producer(str(process_state))

    def test_heartbeat(self):
        print self.__get_process_state()
