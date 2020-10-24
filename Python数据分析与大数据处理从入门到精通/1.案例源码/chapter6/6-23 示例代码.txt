# -*- coding: UTF-8 -*-

from multiprocessing import Process, Queue
import os, time

def set_data(q, tmp_list):
    for item in tmp_list:
        print("当前{0}进程将值 {1} 插入队列".format(os.getpid(), item))
        q.put(item)
        time.sleep(2)

def get_data(q, count):
    for i in range(count):
        value = q.get(True)
        print("当前{0}进程获取到值为：{1}".format(os.getpid(), value))

if __name__ == "__main__":
    que = Queue()
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    length = len(data)
    process1 = Process(target=set_data, args=(que, data))
    process2 = Process(target=get_data, args=(que, length))
    process1.start()
    process2.start()
    process2.join()
    print("主进程执行完毕！")
	
	
	