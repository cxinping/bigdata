#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import sys
import datetime


class Echo(Protocol):
    def connectionMade(self):
        print("Connected to the server!")

    def dataReceived(self, data):
        print("got message: ", data.decode('utf8'))
        reactor.callLater(5, self.say_hello)

    def connectionLost(self, reason):
        print("Disconnected from the server!")

    def say_hello(self):
        if self.transport.connected:
            self.transport.write(
                (u"hello, I'm %s %s" %
                 (sys.argv[1], datetime.datetime.now())).encode('utf-8'))


class EchoClientFactory(ClientFactory):
    def __init__(self):
        self.protocol = None

    def startedConnecting(self, connector):
        print('Started to connect.')

    def buildProtocol(self, addr):
        self.protocol = Echo()
        return self.protocol

    def clientConnectionLost(self, connector, reason):
        print('Lost connection.  Reason:', reason)

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed. Reason:', reason)


host = "127.0.0.1"
port = 8007
factory = EchoClientFactory()
reactor.connectTCP(host, port, factory)
reactor.run()
