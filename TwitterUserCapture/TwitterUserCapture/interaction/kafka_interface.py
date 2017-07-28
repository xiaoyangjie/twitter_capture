# -*- coding:utf-8 -*-
# python2.7
# kafka_interface.py
# kafka接口模块
from pykafka import KafkaClient


# 生产者
class Producer:
    def __init__(self, brokers, topic):
        """
        初始化
        :param brokers: kafka的host，类型string，例子：'192.168.178.221:9092,192.168.178.104:9092,192.168.178.105:9092'
        :param topic: kafka的topic，类型string，例子：'test'
        """
        self.__client = KafkaClient(hosts=brokers)
        self.__topic = self.__client.topics[topic]
        self.__producer = self.__topic.get_producer()
        self.__producer.start()

    def __del__(self):
        self.__producer.stop()

    # 发送信息
    def send_data(self, data):
        self.__producer.produce(bytes(data))
