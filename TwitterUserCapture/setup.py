# -*- coding:utf-8 -*-
from setuptools import setup, find_packages


setup(
    name="TwitterUserCapture",
    version="0.8.0",
    description="capture information of twitter users.",
    author="Junwu He",
    packages=find_packages(),
    install_requires=['pymongo', 'TwitterAPI'],
)
