# -*- coding: utf-8 -*-
from gevent import monkey
import gevent

import random
import time


def task_1(name):
    for i in range(5):
        print(name, i)
        time.sleep(1)  # 协程遇到耗时操作后会自动切换其他协程运行


def task_2(name):
    for i in range(3):
        print(name, i)
        time.sleep(1)


if __name__ == "__main__":
    monkey.patch_all()  # 给所有的耗时操作打上补丁

    gevent.joinall([  # 等到协程运行完毕
        gevent.spawn(task_1, "task_1"),  # 创建协程
        gevent.spawn(task_2, "task_2")
    ])
    print("the main thread!")

