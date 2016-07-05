# -*- coding: utf-8 -*-

from __future__ import division  # must occur at the beginning of the file

import sys
import time

from datetime import datetime

sys.path.append("../")

from userver import *


def total_seconds(td):

    """计算时间间隔秒数,因为timedelta.total_seconds在Python2.7之后才加入标准库中,所以自己实现

    :param td: timedelta类型的对象
    :return: 时间间隔秒数

    """

    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6


def generate_command(device, type, time=0):

    """生成监控设备状态的命令以及定时模式下打开加热器的命令

    命令格式:

    {"mac": {"id": "mac"+current_time, "type": type, ["time": time]}}

    :param device: 设备的物理地址
    :param type: 命令的类型
    :param time: 这个参数只有打开加热器的命令需要, 指定打开加热器的时间
    :return: 无返回值

    """

    if type == 1:

        command = {"id": '+'.join([device, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]), "type": type, "time": time}

    else:

        command = {"id": '+'.join([device, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]), "type": type}

    logging.info("产生命令: %s" % str(command))

    redis_client.lpush(device, command)


def monitor():

    """

    首先查询数据库,获得所有在线设备的物理地址,对每个在线的设备生成监控状态命令

    然后确定每个在线设备的当前模式,如果是定时模式,那么检测当前时间是否达到定时时间,如果到了,生成打开加热器的命令

    这个程序每一分钟执行一次

    :return: 无返回值

    """

    logging.info("后台启动")

    while 1:

        connection = pymysql.connect(host="", user="", passwd="", db="")

        cursor = connection.cursor()

        cursor.execute("SELECT mac, mode, start, last, online FROM device where online = 1")

        logging.debug(cursor._last_executed)

        devices = cursor.fetchall()

        logging.info("%s 台设备在线" % len(devices))

        for device in devices:

            MAC = device[0]

            generate_command(MAC, 2)

            generate_command(MAC, 4)

            generate_command(MAC, 6)

            generate_command(MAC, 7)

            mode = device[1]

            if mode == 1:

                logging.info("定时模式")

                start_datetime = device[2]

                start_date = start_datetime.date()

                current_datetime = datetime.combine(start_date, datetime.now().time())

                last_time = device[3]

                interval = total_seconds(start_datetime - current_datetime)

                logging.info("时间间隔秒数%d" % interval)

                if -60 < interval < 60:

                    generate_command(MAC, 1, last_time)

            elif mode == 0:

                logging.info("普通模式")

        cursor.close()

        connection.close()

        logging.info("准备休眠")

        time.sleep(60)


if __name__ == "__main__":

    monitor()
