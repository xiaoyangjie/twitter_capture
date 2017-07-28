# -*- coding:utf-8 -*-
# python2.7
# tool.py
# 工具函数集
import re


# list切分生成器
def list_split(raw_list, num=None, length=1):
    """
    将一个list切分成多个list
    :param raw_list: 带切分list
    :param num: 切分个数，若有则尽量等分，若无则用length切分
    :param length: 切分长度，每一段截取length长度的list
    :return: 切分后list的迭代器
    """
    if num is not None:
        length = len(raw_list) / num + 1
    for i in xrange(0, len(raw_list), length):
        if i + length > len(raw_list):
            yield raw_list[i:]
        else:
            yield raw_list[i:i + length]


# 判断是否是screen_name格式
def is_screen_name(instance):
    if not isinstance(instance, (str, unicode)):
        return False
    pattern = re.compile(r"^\w+$")
    if pattern.match(instance):
        return True
    return False


# 判断是否是用户ID格式
def is_account_id(instance):
    if isinstance(instance, (int, long)) and instance > 0:
        return True
    return False


if __name__ == "__main__":
    l = [0, 1, 2, 3, 4]
    for sub in list_split(l, length=100):
        print sub