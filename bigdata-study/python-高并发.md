

### Python简单实现线程顺序执行与线程并发执行

```
from threading import Thread
import time
from time import ctime,sleep

def my_counter():

    for i in range(2):
        i+=1
    sleep(1)
    return True

def main():
    thread_array={}
    start_time=time.time()
    for tid in range(2):
        t=Thread(target=my_counter)
        t.start()
        thread_array[tid]=t
        print"%s is running thread_array[%d]"%(ctime(),tid)
    for i in range(2):
        thread_array[i].join()
        print"%s thread_array[%d] ended"%(ctime(),i)
    end_time=time.time()
    print("Total time:{}".format(end_time-start_time))

if __name__=='__main__':
    main()


```