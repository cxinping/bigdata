from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
import datetime


def long_operation(protocol):
    import time
    time.sleep(5)
    print("%s: The protocol %s has been started for 5 seconds." %
          (datetime.datetime.now(), protocol))


class Echo(DatagramProtocol):  # 定义DatagramProtocol子类
    def startProtocol(self):
        print(datetime.datetime.now(), ": started")
        # 调用long_operation()函数，使其在辅线程中执行。本调用在主线程中立即返回
        reactor.callInThread(long_operation, self)


protocol = Echo()  # 实例化Protocol子类

reactor.listenUDP(8007, protocol)
reactor.run()  # 挂起运行
