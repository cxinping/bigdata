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
    def datagramReceived(self, data, address):
        print("Got data from: %s: %s" % (address, data.decode('utf8')))


protocol = Echo()  # 实例化Protocol子类


def routine():  # 每隔5秒向服务器发送消息
    time.sleep(1)
    while True:
        protocol.transport.write(("%s: say hello to myself." %
                                  (datetime.datetime.now(), )).encode('utf-8'),
                                 (host, port))
        time.sleep(5)


threading.Thread(target=routine).start()
reactor.listenUDP(port, protocol)
reactor.run()  # 挂起运行
