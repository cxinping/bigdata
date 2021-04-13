#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet import reactor, defer


def printSquare(d):  # 正常处理函数
    print("Square of %d is %d" % (d, d * d))
    return d


def printTwice(d):  # 正常处理函数
    print("Twice of %d is %d" % (d, 2 * d))
    return d


def makeDefer():
    d = defer.Deferred()  # 定义Defer实例
    d.addCallback(printSquare)  # 添加正常处理回调函数
    d.addCallback(printTwice)  # 添加正常处理回调函数
    reactor.callLater(2, d.callback, 5)  # 配置延时调用


makeDefer()
reactor.run()  # 挂起运行
