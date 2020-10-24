# -*- coding: UTF-8 -*-

from multiprocessing import Pool
import time
def proc(num):
    print("当前进程编号：{0} 开始执行".format(num))
    time.sleep(5)
    print("当前进程编号：{0} 执行结束".format(num))
if __name__ == "__main__":
    pool = Pool(3)
    print("创建5个进程")
    for i in range(5):
        process_num = "{0}".format(i)
        pool.apply_async(proc, args=(process_num,))
    pool.close()
    pool.join()
    print("主进程执行完毕！")

	
	