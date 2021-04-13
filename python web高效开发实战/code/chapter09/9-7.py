#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet import defer
from twisted.python import failure
import sys

d = defer.Deferred()  # 定义Defer实例

# ############以下是Defer回调函数添加阶段############################


def printSquare(d):  # 正常处理函数
    print("Square of %d is %d" % (d, d * d))


def processError(f):  # 错误处理函数
    print("error when process ")


d.addCallback(printSquare)  # 添加正常处理回调函数
d.addErrback(processError)  # 添加错误处理回调函数

# ##############以下是Defer调用阶段######################################
if len(sys.argv) > 1 and sys.argv[1] == "call_error":
    f = failure.Failure(Exception("my exception"))
    d.errback(f)  # 调用错误处理函数processError
else:
    d.callback(4)  # 调用正常处理函数printSquare(4)
