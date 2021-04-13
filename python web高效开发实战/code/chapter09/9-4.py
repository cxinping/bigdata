#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet.protocol import DatagramProtocol
import socket
from twisted.internet import reactor


class Echo(DatagramProtocol):
    def datagramReceived(self, data, address):
        print(data.decode('utf8'))


address = ("127.0.0.1", 8008)

recvSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
recvSocket.setblocking(False)  # 设为阻塞模式
recvSocket.bind(address)
reactor.adoptDatagramPort(recvSocket.fileno(), socket.AF_INET, Echo())
recvSocket.close()

# 新建一个socket作为发送端
sendSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sendSocket.sendto("Hello my friend!".encode('utf-8'), address)
reactor.run()
