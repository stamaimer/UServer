# -*- coding: utf-8 -*-

"""tcp_server

这个程序创建一个TCP服务器,监听`9009`端口.设备发起链接请求视为上线,程序针对每个链接新建一个协程,用于转发用户命令,监控设别状态,处理设备上报信息.

"""

import re
import sys
import ast
import json
import struct

import gevent

from binascii import hexlify
from datetime import datetime

from gevent.server import StreamServer
from gevent import socket

sys.path.append("../")

from userver import *

socket.setdefaulttimeout(5)

greenlets = {}
"""list: 全局变量`greenlets`

存储设备和设备对应的链接,用来分辨是否是新设备

"""


def translate(data, flag="server"):

    """把一个十六进制字符串以两位为整体进行切割,同时把切割结果转换成为十进制整数

    :param data: 十六进制字符串
    :param flag: 由于程序产生字符串,而设备发送二进制串,需要使用标志区分数据来源,当数据来自设备,需要先使用`binascii.hexlify`进行格式转换.取值范围:{"server"(默认值), "client"}
    :return: 返回一个整数列表

    """

    if flag == "client":

        data = hexlify(data)

    tmp = []

    for i in xrange(len(data) / 2):

        tmp.append(int(data[i * 2: i * 2 + 2], 16))

    return tmp


def get_checksum(data):

    """检验设备响应数据的校验和

    首先调用`translate`函数获得响应数据的整数列表表示,然后对这个列表的除了最后一个元素之外所有元素求和,求和结果与`0xff`做位与操作,所得结果即为校验和,最后与整数列表的末尾元素比较,一致返回`True`,否则返回`False`

    :param data: 设备响应数据(二进制串)
    :return: 如果计算结果与设备上报的校验和一致返回`True`,否则返回`False`

    """

    data = translate(data, "client")

    checksum = sum(data[:-1]) & 0xff

    return 1 if data[-1] == checksum else 0


def set_checksum(data):

    """计算即将发送的命令的校验和并附加到命令末尾

    首先调用`translate`函数获得命令的整数列表表示,然后对这个列表的所有元素求和,求和结果与`0xff`做位与操作,所得结果即为校验和,把校验和附加到整数列表的末尾,最后把整数列表转换为二进制串

    :param data: 即将发送的命令
    :return: 带有校验和的命令

    """

    data = translate(data)

    checksum = sum(data) & 0xff

    data.append(checksum)

    sequence = map(lambda x: struct.pack("!B", x), data)

    return ''.join(sequence)


def handle_report(MAC, socket, connection, cursor, data):

    """处理上报信息

    首先检验上报信息的校验和,如果校验失败,发送`f5aa0802`命令,然后检查命令类型,如果命令类型不符,发送`f5aa0801`,如果上述两个检查通过,发送`f5aa0800`
    然后根据协议处理上报事件:
    
        如果事件类型为`1`,表示读取药量错误(设备接触不良);
        如果事件类型为`2`,表示用户按下设备物理按键,然后根据按键位置的不同相应的更新`power`字段;
        如果事件类型为`3`,表示药水已经用完;
        如果事件类型为`4`,表示药水已经更新,调用读取药量函数,更新数据;

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param data: 设备上报数据
    :return: 根据上报数据执行某项操作, 无返回值

    """

    logging.info("%s 处理上报信息" % MAC)

    data = data[:6]

    if get_checksum(data):

        data = translate(data, "client")

        if data[2] == 8:

            command = "f5aa0800"

            if data[3] == 1:

                logging.error("%s 读取药水错误" % MAC)

            if data[3] == 2:

                key = data[4]

                if key == 0:

                    cursor.execute("UPDATE device SET power = 0 WHERE mac = %s", MAC)

                else:

                    cursor.execute("UPDATE device SET power = %s WHERE mac = %s", (2 * key, MAC))

                logging.debug(cursor._last_executed)

                logging.info("%s 用户按下%s键" % (MAC, key))

            if data[3] == 3:

                cursor.execute("UPDATE device SET dosage = 0 WHERE mac = %s", MAC)

                logging.debug(cursor._last_executed)

                logging.warning("%s 药水已经用完" % MAC)

            if data[3] == 4:

                read_remaining_potion(MAC, socket, connection, cursor)

                logging.info("%s 药水已经更新" % MAC)

        else:

            command = "f5aa0801"

            logging.error("%s 上报命令错误" % MAC)

    else:

        command = "f5aa0802"

        logging.error("%s 上报校验错误" % MAC)

    send_command(MAC, socket, connection, cursor, command)


def send_command(MAC, socket, connection, cursor, command, response_length=0, response_type=0, times=5):

    """通过链接发送命令,等待响应,返回有效响应数据

    这个函数默认发送五次命令,如果其中有一次发送成功,同时没有其它错误,则返回有效响应数据(删除响应头,命令类型以及校验和),结束循环,函数返回.

    下面介绍每次循环的流程:

        首先通过链接尝试发送数据,如果出现异常,关闭连接,清理资源;
        然后通过链接尝试接收数据,如果出现异常,关闭连接,清理资源,如果等待超时,进入下次循环,重新发送命令;
        获得响应数据,如果响应数据为空,关闭连接,清理资源;
        然后检验响应数据类型是否与期望相符,如果相符,则验证校验和,如果校验通过,返回有效响应数据,否则进入下次循环,重新发送命令;
        如果响应数据类型与期望不符,把设备响应信息当作上报信息处理

        *因为处理上报信息过程中发送的命令无需返回,所以发送之后,函数直接返回,不进行后续操作*

    如果五次循环过后,仍未获得有效响应数据,关闭链接,清理资源

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param command: 待发送的命令
    :param response_length: 因为设备返回的响应数据不遵循标准,所以需要根据协议手动获取有效响应数据
    :param response_type: 期望的响应数据的类型,用来检查响应数据是否是上报信息
    :param times: 循环发送命令的次数,默认值`5`
    :return: 如果一切正常,返回有效响应数据;如果指定循环次数过后,仍未获得有效响应数据,返回`None`

    """

    for _ in xrange(times):

        try:

            socket.send(set_checksum(command))

        except:

            cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.critical(sys.exc_info()[1][1])

            cursor.close()

            connection.close()

            socket.close()

            return

        if response_length == 0 and response_type == 0:

            return

        else:

            while 1:

                logging.info("%s 尝试接收数据" % MAC)

                try:

                    response = socket.recv(4096)

                except gevent.socket.timeout:

                    break

                except gevent.socket.error:

                    cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

                    logging.debug(cursor._last_executed)

                    logging.critical(sys.exc_info()[1][1])

                    cursor.close()

                    connection.close()

                    socket.close()

                    return

                logging.info("%s 发送 %s" % (MAC, str(hexlify(response[:response_length]))))

                if response:

                    data = translate(response, "client")

                    if data[2] != response_type:

                        handle_report(MAC, socket, connection, cursor, response)

                        break

                    else:

                        if not get_checksum(response[:response_length]):

                            logging.error("%s 响应校验出错" % MAC)

                            break

                        else:

                            return data[3:-1]
                else:

                    cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

                    logging.debug(cursor._last_executed)

                    logging.critical("%s 设备返回空值" % MAC)

                    cursor.close()

                    connection.close()

                    socket.close()

                    return

    cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

    logging.debug(cursor._last_executed)

    logging.critical("%s 等待响应超时" % MAC)

    cursor.close()

    connection.close()

    socket.close()

    return None


def return_status(key, code, msg):

    """返回设备执行用户命令的结果

    :param key: 用户命令唯一标志
    :param code: 执行结果的状态,`0` 表示执行成功,`1` 表示执行错误
    :param msg: 执行结果的信息
    :return: None

    """

    redis_client.rpush(key, json.dumps({"code": code, "msg": msg}))


def test_connection(MAC, socket, connection, cursor):

    """设备上电连接至服务器，服务器端立刻发送这个命令，并且打开蜂鸣器一声响，提示用户设备已经连接至服务器

    由于设备问题时常断连,然后重连,现在只在新的设备首次发起链接之时发送这个命令

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa010001"

    response = send_command(MAC, socket, connection, cursor, command, 5, 1)

    if type(response) == list:

        logging.info("%s 测试链接成功" % MAC)

    else:

        logging.critical("%s 测试链接失败" % MAC)


def heartbeat(MAC, socket, connection, cursor, task={}):

    """心跳命令,检测设备是否在线,每一分钟检测一次,这个命令后台生成,属于监控设备状态的命令之一

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 由于这个命令后台生成,所以`task`为空
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa010000"

    response = send_command(MAC, socket, connection, cursor, command, 5, 1)

    if type(response) == list:

        cursor.execute("UPDATE device SET online = 1 WHERE mac = %s", MAC)

        logging.debug(cursor._last_executed)

        logging.info("%s 心跳测试成功" % MAC)

    else:

        logging.critical("%s 心跳测试失败" % MAC)


def check_status(MAC, socket, connection, cursor, task={}):

    """检查加热器当前状态的命令,每一分钟检测一次,这个命令后台生成,属于监控设备状态的命令之一

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 由于这个命令后台生成,所以`task`为空
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa030000"

    response = send_command(MAC, socket, connection, cursor, command, 8, 3)

    if type(response) == list:

        if response[0] == 0:

            if response[1] == 0:

                cursor.execute("UPDATE device SET power = %s WHERE mac = %s", (0, MAC))

            else:

                cursor.execute("UPDATE device SET power = %s WHERE mac = %s", (response[2], MAC))

            logging.debug(cursor._last_executed)

            logging.info("%s 开关状态: %s; 应开时间: %s; 已开时间: %s" % (MAC, response[1], response[2], response[3]))

        elif response[0] == 1:

            logging.error("%s 命令校验出错" % MAC)

        elif response[0] == 2:

            cursor.execute("UPDATE device SET dosage = -1 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已被拔出" % MAC)

        elif response[0] == 3:

            cursor.execute("UPDATE device SET dosage = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已经用完" % MAC)

        elif response[0] == 4:

            logging.error("%s 读取药水错误" % MAC)

    else:

        logging.critical("%s 状态查询失败" % MAC)


def turnon(MAC, socket, connection, cursor, task):

    """打开加热器的命令,这个命令由用户通过移动应用发送,发送该命令的时候需要指定加热时长

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 打开加热器命令的具体内容(json类型)
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    if task["time"] not in [1, 2, 3, 4, 5, 6, 7, 8]:

        logging.error("%s 加热持续时间超出预定范围" % MAC)

        return_status(task["id"], 1, "%s 加热持续时间超出预定范围" % MAC)

        return

    command = "f5aa0301" + '0' + str(task["time"])

    response = send_command(MAC, socket, connection, cursor, command, 8, 3)

    if type(response) == list:

        if response[0] == 0:

            cursor.execute("UPDATE device SET power = %s WHERE mac = %s", (task["time"], MAC))

            logging.debug(cursor._last_executed)

            logging.info("%s 设备已经打开" % MAC)

            return_status(task["id"], 0, "%s 设备已经打开" % MAC)

        elif response[0] == 1:

            logging.error("%s 命令校验出错" % MAC)

            return_status(task["id"], 1, "%s 命令校验出错" % MAC)

        elif response[0] == 2:

            cursor.execute("UPDATE device SET dosage = -1 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已被拔出" % MAC)

            return_status(task["id"], 1, "%s 药水已被拔出" % MAC)

        elif response[0] == 3:

            cursor.execute("UPDATE device SET dosage = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已经用完" % MAC)

            return_status(task["id"], 1, "%s 药水已经用完" % MAC)

        elif response[0] == 4:

            logging.error("%s 读取药水错误" % MAC)

            return_status(task["id"], 1, "%s 读取药水错误" % MAC)

    else:

        logging.critical("%s 设备开启失败" % MAC)

        return_status(task["id"], 1, "%s 设备开启失败" % MAC)


def turnof(MAC, socket, connection, cursor, task):

    """关闭加热器的命令,这个命令由用户通过移动应用发送

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 关闭加热器命令的具体内容(json类型)
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa030100"

    response = send_command(MAC, socket, connection, cursor, command, 8, 3)

    if type(response) == list:

        if response[0] == 0:

            cursor.execute("UPDATE device SET power = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.info("%s 设备已经关闭" % MAC)

            return_status(task["id"], 0, "%s 设备已经关闭" % MAC)

        elif response[0] == 1:

            logging.error("%s 命令校验出错" % MAC)

            return_status(task["id"], 1, "%s 命令校验出错" % MAC)

        elif response[0] == 2:

            cursor.execute("UPDATE device SET dosage = -1 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已被拔出" % MAC)

            return_status(task["id"], 1, "%s 药水已被拔出" % MAC)

        elif response[0] == 3:

            cursor.execute("UPDATE device SET dosage = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已经用完" % MAC)

            return_status(task["id"], 1, "%s 药水已经用完" % MAC)

        elif response[0] == 4:

            logging.error("%s 读取药水错误" % MAC)

            return_status(task["id"], 1, "%s 读取药水错误" % MAC)

    else:

        logging.critical("%s 设备关闭失败" % MAC)

        return_status(task["id"], 1, "%s 设备关闭失败" % MAC)


def delete(MAC, socket, connection, cursor, task):

    """删除设备命令,这个命令由用户通过移动应用发送,从全局变量`greenlets`以及数据库删除这个设备

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 删除设备命令的具体内容(json类型)
    :return: 无返回值

    """

    del greenlets[MAC]

    cursor.execute("DELETE FROM device WHERE mac = %s", MAC)

    logging.debug(cursor._last_executed)

    logging.info("%s 设备已被删除" % MAC)

    return_status(task["id"], 0, "%s 设备已被删除" % MAC)

    cursor.close()

    connection.close()

    socket.close()


def read_temperature_humidity(MAC, socket, connection, cursor, task={}):

    """读取设备所处环境的温度和湿度的命令,每一分钟读取一次,这个命令后台生成,属于监控设备状态的命令之一

    具体的温度和湿度解析规则参见<<设备控制通信协议>>

    其中温度等于`-273`和湿度等于`0`表示传感器有故障

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 由于这个命令后台生成,所以`task`为空
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa04"

    response = send_command(MAC, socket, connection, cursor, command, 9, 4)

    if type(response) == list:

        if response[0] == 0:

            temp_high_byte = "{0:08b}".format(response[1])

            flag = temp_high_byte[0]

            temp_high_byte = int(temp_high_byte[1:], 2)

            temperature = (temp_high_byte * 256 + response[2]) / float(10)

            if flag == '1':

                temperature = -temperature

            humidity = (response[3] * 256 + response[4]) / float(10)

            cursor.execute("UPDATE device SET temperature = %s, humidity = %s WHERE mac = %s", (temperature, humidity, MAC))

            logging.debug(cursor._last_executed)

            logging.info("%s 温度湿度读取成功" % MAC)

        elif response[0] == 1:

            cursor.execute("UPDATE device SET temperature = -273, humidity = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.error("%s 传感器有故障" % MAC)

        elif response[0] == 2:

            logging.error("%s 命令校验出错" % MAC)

    else:

        logging.critical("%s 温度湿度读取失败" % MAC)


def read_remaining_potion(MAC, socket, connection, cursor, task={}):

    """读取剩余药量的命令,每一分钟读取一次,这个命令后台生成,属于监控设备状态的命令之一

    具体的药量解析规则参见<<设备控制通信协议>>

    其中药量等于`-1`表示并未插入药水

    :param MAC: 设备的物理地址(唯一标志)
    :param socket: 设备和程序之间的链接
    :param connection: 当前协程的数据库链接(每个协程使用独立的数据库链接)
    :param cursor:  当前协程的数据库游标(每个协程使用独立的数据库游标)
    :param task: 由于这个命令后台生成,所以`task`为空
    :return: 根据响应数据,执行某项操作,无返回值(具体规则参见<<设备控制通信协议>>)

    """

    command = "f5aa07"

    response = send_command(MAC, socket, connection, cursor, command, 7, 7)

    if type(response) == list:

        if response[0] == 0:

            total = 150 * 60

            remain = (total - (response[1] * 60 + response[2])) / float(total)

            remain = round(remain, 2)

            cursor.execute("UPDATE device SET dosage = %s WHERE mac = %s", (remain, MAC))

            logging.debug(cursor._last_executed)

            logging.info("%s 药量读取成功" % MAC)

        elif response[0] == 1:

            logging.error("%s 命令校验出错" % MAC)

        elif response[0] == 2:

            cursor.execute("UPDATE device SET dosage = -1 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已被拔出" % MAC)

        elif response[0] == 3:

            cursor.execute("UPDATE device SET dosage = 0 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            logging.warning("%s 药水已经用完" % MAC)

        elif response[0] == 4:

            logging.error("%s 读取药水错误" % MAC)

    else:

        logging.critical("%s 药量读取失败" % MAC)


def handle(socket, address):

    """每个协程运行的主函数,每当某个设备发起链接,服务器创建一个新的协程运行该函数

    这个函数首先检测是否是有效设备发起的链接,正常设备发起链接,首先需要上报该设备的物理地址,通过这个检测之后,然后使用全局变量`greenlets`以及数据库来确定该设备的具体情况

    接下来是这个函数的主要部分: 一个无限循环,在循环过程中转发用户命令,监控设备状态,处理设备上报信息

    单次循环具体流程描述如下:

        首先检查`redis`是否含有发向当前设备的命令:

            如果有,执行相应命令;
            如果没有,则尝试接收设备的上报信息(具体流程参见`send_command`);
            执行下次循环;

    :param socket: 设备和程序之间的链接
    :param address: 设备具体`IP`地址以及端口
    :return: 无返回值

    """

    logging.info("设备接入: %s" % socket)

    try:

        MAC = socket.recv(4096)

    except:

        logging.critical(sys.exc_info()[1][1])

        logging.critical("接收数据失败")

        socket.close()

        return

    if not re.match("[0-9A-F]{12}", MAC):

        logging.critical("物理地址无效: %s" % MAC)

        socket.close()

        return

    logging.info("物理地址: %s" % MAC)

    # time.sleep(2)
    #
    # logging.info("发送测试命令: %s" % MAC)  #
    #
    # test_connection(MAC, socket, connection, cursor)  #

    connection = pymysql.connect(host="120.25.205.115", user="root", passwd="86564d7071", db="hxd2")

    connection.autocommit(1)

    cursor = connection.cursor()

    if MAC not in greenlets:

        greenlets[MAC] = gevent.getcurrent()

        cursor.execute("SELECT id FROM device WHERE mac = %s", MAC)

        logging.debug(cursor._last_executed)

        if not cursor.fetchall():

            logging.info("新的设备: %s" % MAC)

            logging.info("发送测试命令: %s" % MAC)  #

            test_connection(MAC, socket, connection, cursor)  #

            current_time = str(datetime.now()).split('.')[0]

            cursor.execute("INSERT INTO device (mac, online, ctime, utime, ip) VALUES (%s, %s, %s, %s, %s)", (MAC, 1, current_time, current_time, address[0]))

            logging.debug(cursor._last_executed)

        else:

            logging.info("程序重启")

            cursor.execute("UPDATE device SET online = 1 WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

    else:

        greenlets[MAC] = gevent.getcurrent()

        logging.info("旧的设备: %s" % MAC)

        cursor.execute("UPDATE device SET online = 1 WHERE mac = %s", MAC)

        logging.debug(cursor._last_executed)

    while 1:

        connection.ping()

        task = redis_client.rpop(MAC)

        if not task:

            cursor.execute("SELECT online FROM device WHERE mac = %s", MAC)

            logging.debug(cursor._last_executed)

            online = cursor.fetchone()[0]

            logging.info("%s 设备当前状态: %s" % (MAC, online))

            if online:

                logging.info("%s 尝试接收数据" % MAC)

                try:

                    data = socket.recv(4096)

                except gevent.socket.timeout:

                    continue

                except gevent.socket.error:

                    cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

                    logging.debug(cursor._last_executed)

                    logging.critical(sys.exc_info()[1][1])

                    cursor.close()

                    connection.close()

                    socket.close()

                    return

                logging.info("%s 发送 %s" % (MAC, data))

                if data:

                    handle_report(MAC, socket, connection, cursor, data)

                else:

                    cursor.execute("UPDATE device SET online = 0 WHERE mac = %s", MAC)

                    logging.debug(cursor._last_executed)

                    logging.critical("设备返回空值")

                    cursor.close()

                    connection.close()

                    socket.close()

                    return

            else:

                logging.critical("设备已经离线")

                cursor.close()

                connection.close()

                socket.close()

                return

        else:

            task = ast.literal_eval(task)

            logging.info("命令: %s" % str(task))

            if task["type"] == -1:

                delete(MAC, socket, connection, cursor, task)

                return

            elif task["type"] == 0:

                turnof(MAC, socket, connection, cursor, task)

                continue

            elif task["type"] == 1:

                turnon(MAC, socket, connection, cursor, task)

                continue

            elif task["type"] == 2:

                heartbeat(MAC, socket, connection, cursor, task)

                continue

            elif task["type"] == 4:

                read_temperature_humidity(MAC, socket, connection, cursor, task)

                continue

            elif task["type"] == 6:

                check_status(MAC, socket, connection, cursor, task)

                continue

            elif task["type"] == 7:

                read_remaining_potion(MAC, socket, connection, cursor, task)

                continue


if __name__ == "__main__":

    StreamServer(("0.0.0.0", 9009), handle, backlog=256, spawn=65536).serve_forever()

