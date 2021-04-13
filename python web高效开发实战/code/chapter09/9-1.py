#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol

clients = []


class Spreader(Protocol):
    def __init__(self, factory):
        self.factory = factory
        self.connect_id = None

    def connectionMade(self):
        self.factory.numProtocols = self.factory.numProtocols + 1
        self.connect_id = self.factory.numProtocols
        self.transport.write((u"欢迎来到Spread Site, 您是第%d个客户端用户！\n" %
                              (self.connect_id, )).encode('utf8'))
        print("new connect: %d" % self.connect_id)
        clients.append(self)

    def connectionLost(self, reason):
        clients.remove(self)
        print("lost connect: %d" % self.connect_id)

    def dataReceived(self, data):
        print("dataReceived() entered!")
        if data == "close":
            self.transport.loseConnection()
            print("%s closed" % self.connect_id)
        else:
            print("spreading message from %s : %s" % (self.connect_id, data))
            for client in clients:
                if client != self:
                    client.transport.write(data)
        print("dataReceived() existed!")


from twisted.internet.protocol import Factory
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor


class SpreadFactory(Factory):
    def __init__(self):
        self.numProtocols = 0

    def buildProtocol(self, addr):
        return Spreader(self)


# 8007是本服务器的监听端口，建议选择大于1024的端口
endpoint = TCP4ServerEndpoint(reactor, 8007)
endpoint.listen(SpreadFactory())
reactor.run()  # 挂起运行
