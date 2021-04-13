# !/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet import defer

d = defer.Deferred()  # 定义Defer实例


def printSquare(d):  # 正常处理函数
    print("Square of %d is %d" % (d, d * d))
    return d


def processError(f):  # 错误处理函数
    print("error when process ")


def printTwice(d):
    print("Twice of %d is %d" % (d, 2 * d))
    return d


d.addCallback(printSquare)  # 添加正常处理回调函数
d.addErrback(processError)  # 添加错误处理回调函数
d.addCallback(printTwice)  # 添加第2个正常处理回调函数

d.callback(5)
