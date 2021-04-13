#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import threading
import time
import datetime

host = "127.0.0.1"
port = 8007


class Echo(DatagramProtocol):  # 定义DatagramProtocol子类
    def startProtocol(self):  # 连接成功后被调用
        self.transport.connect(host, port)  # 指定对方地址/端口
        self.transport.write(b"Here is the first connected message")
        print("Connection created!")

    def datagramReceived(self, data, address):  # 收到数据时被调用
        print(data.decode('utf8'))

    def connectionRefused(self):  # 每次通信失败后被调用
        print("sent failed!")

    def stopProtocol(self):
        print("Connection closed!")


protocol = Echo()  # 实例化Protocol子类


def routine():  # 每隔5秒向服务器发送消息
    time.sleep(1)
    while True:
        protocol.transport.write(("%s: say hello to myself." %
                                  (datetime.datetime.now(), )).encode('utf-8'))
        time.sleep(5)


threading.Thread(target=routine).start()
reactor.listenUDP(port, protocol)
reactor.run()  # 挂起运行
