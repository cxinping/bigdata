
# 协程

协程简绍

所谓「异步 IO」，就是你发起一个 IO 操作，却不用等它结束，你可以继续做其他事情，当它结束时，你会得到通知。

Asyncio 是并发（concurrency）的一种方式。对 Python 来说，并发还可以通过线程（threading）和多进程（multiprocessing）来实现。

Asyncio 并不能带来真正的并行（parallelism）。当然，因为 GIL（全局解释器锁）的存在，Python 的多线程也不能带来真正的并行。

可交给 asyncio 执行的任务，称为协程（coroutine）。一个协程可以放弃执行，把机会让给其它协程（即 yield from 或 await）。

## 定义协程

协程的定义，需要使用 async def 语句。

> async def do_some_work(x): pass

do_some_work 便是一个协程。
准确来说，do_some_work 是一个协程函数，可以通过 asyncio.iscoroutinefunction 来验证：

> print(asyncio.iscoroutinefunction(do_some_work)) # True

准确来说，do_some_work 是一个协程函数，可以通过 asyncio.iscoroutinefunction 来验证：

> print(asyncio.iscoroutinefunction(do_some_work)) # True

```
async def do_somework(x):
    print("Waiting " + str(x))
    await asyncio.sleep(x)
```

asyncio.sleep 也是一个协程，所以 await asyncio.sleep(x) 就是等待另一个协程。

## 运行协程

调用协程函数，协程并不会开始运行，只是返回一个协程对象，可以通过 asyncio.iscoroutine 来验证：

```
print(asyncio.iscoroutine(do_some_work(3))) # True
```

此处还会引发一条警告

```
/Users/leicx/wangshuo/workspace/demo1/test1/test_async1.py:9: RuntimeWarning: coroutine 'do_somework' was never awaited
  print(asyncio.iscoroutine(do_somework(3)))
```

要让这个协程对象运行的话，有两种方式

* 在另一个已经运行的协程中用 `await` 等待它
* 通过 `ensure_future` 函数计划它的执行

简单来说，只有 loop 运行了，协程才可能运行。
下面先拿到当前线程缺省的 loop ，然后把协程对象交给 loop.run_until_complete，协程对象随后会在 loop 里得到运行。

```
loop = asyncio.get_event_loop()
loop.run_until_complete(do_some_work(3))
```

run_until_complete 是一个阻塞（blocking）调用，直到协程运行结束，它才返回。这一点从函数名不难看出。

run_until_complete 的参数是一个 future，但是我们这里传给它的却是协程对象，之所以能这样，是因为它在内部做了检查，通过 ensure_future 函数把协程对象包装（wrap）成了 future。所以，我们可以写得更明显一些：

完整代码

```
import asyncio
import time

async def do_somework(x):
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime() ))
    print("Waiting " + str(x))
    await asyncio.sleep(x)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime() ))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_somework(3))
```




