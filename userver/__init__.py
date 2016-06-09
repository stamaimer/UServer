# -*- coding: utf-8 -*-

"""UServer

这个项目负责通过设备与程序之间的链接转发用户命令,监控设备状态,处理设备上报信息.

"""

from gevent import monkey

monkey.patch_all()

import sys
import logging

import redis
import pymysql

pymysql.install_as_MySQLdb()

redis_client = redis.Redis("120.25.205.115", 6379, password="86564d7071", db=0)

logging.basicConfig(format="%(asctime)s - %(funcName)s - %(levelname)s - %(message)s", stream=sys.stdout, level=logging.INFO)

# connection = pymysql.connect(host="120.25.205.115", user="root", passwd="86564d7071", db="hxd2")
#
# cursor = connection.cursor()
#
# redis_client.flushall()
