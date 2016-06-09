# -*- coding: utf-8 -*-

import sys
import gevent
import socket
import random
import string

import gevent.monkey

gevent.monkey.patch_all()

sys.path.append("../")

from userver import *

candidates = string.digits + "ABCDEF"
"""string: 生成物理地址的候选集

使用这个集合产生符合验证规则的物理地址

"""


def test(pid):

    """模拟设备链接

    创建一个`socket`链接,然后发送模拟物理地址.

    真实模拟设备需要实现<<设备控制通信协议>>中的各项协议

    :param pid: 当前协程序号
    :return: 无返回值

    """

    MAC = ''.join([random.choice(candidates) for _ in xrange(12)])

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    client.connect(("120.25.205.115", 9009))

    client.send(MAC)

    logging.info(pid)


if __name__ == "__main__":

    gevents = [gevent.spawn(test, i) for i in xrange(1000)]

    gevent.joinall(gevents)
