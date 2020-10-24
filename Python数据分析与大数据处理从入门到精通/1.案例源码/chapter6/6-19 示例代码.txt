# -*- coding: UTF-8 -*-

import time
from multiprocessing import Process
import os

def new_process(para):
    time.sleep(10)
    print("子进程ID：{0}".format(os.getpid()))
    print("主进程传递来的参数：{0}".format(para))

if __name__ == "__main__":
    print("父进程ID是：{0}".format(os.getpid()))
    process = Process(target=new_process, args=("主进程参数",))
    process.start()
    process.join()
    print("主进程执行完毕！")

	
	