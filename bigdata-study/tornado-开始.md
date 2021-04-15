# Tornado

## 参考资料
```	
http://shouce.jb51.net/tornado/ch1.html#ch1-1

tornado中@tornado.web.asynchronous装饰器使用介绍
http://blog.sina.com.cn/s/blog_172bac9430102x0vl.html

生成器，函数，数组
https://www.cnblogs.com/shijingjing07/p/6478539.html

```	

## 安装Tornado

```	
pip3 install tornado
```

## Tornado Web

### 例子1

```
# -*- coding: utf-8 -*-

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        greeting = self.get_argument('greeting', 'Hello')
        self.write(greeting + ', friendly user!')

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/", IndexHandler)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
```

在浏览器中打开http://localhost:8000


或者打开另一个终端窗口使用curl测试我们的应用：

```
$ curl http://localhost:8000/
Hello, friendly user!
$ curl http://localhost:8000/?greeting=Salutations
Salutations, friendly user!
```

### 例子2

```
import tornado.ioloop
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello world")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

def main():
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
```   

# 异步和协程

## 同步和异步
> 同步 I/O 操作（synchronous I/O operation）导致请求进程阻塞，直到 I/O 操作完成

> 异步 I/O 操作（asynchronous I/O operation）不导致请求进程阻塞 

在Python中，同步I/O操作被理解为一个可被调用的I/O函数会阻塞调用函数的执行，异步 I/O操作不会阻塞调用函数的执行。

### 同步例子

```
f# -*- coding: utf-8 -*-
from tornado.httpclient import HTTPClient, AsyncHTTPClient
import time

def synchronous_visit():
    http_client = HTTPClient()
    response = http_client.fetch('http://www.163.com')
    time.sleep(3)
    print(response.body)

if __name__ == '__main__':
    synchronous_visit()
    print('--- end ---')
```
### 异步例子

```
# -*- coding: utf-8 -*-
import time
from tornado.httpclient import AsyncHTTPClient
import asyncio

async def asynchronous_fetch(url):
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(url)
    await asyncio.sleep(5)
    print(response.body)

    return response.body

if __name__ == '__main__':
    print('--- end ---')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asynchronous_fetch('http://www.163.com'))
```

异步I/O访问 http://www.163.com 网站的函数无需等待访问完成再返回

## 协程
使用Tornado协程可以开发出类似同步代码的异步行为，协程本身不使用线程，所以减少了线程上下文切换的开销，是一种更高效的开发模式。

1， 编写协程函数
使用协程技术开发网页访问功能

```
# -*- coding: utf-8 -*-
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
import asyncio

@gen.coroutine
def coroutime_visit():
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch("http://www.163.com")
    print(response.body)


loop = asyncio.get_event_loop()
loop.run_until_complete(coroutime_visit())
```

2, 调用协程函数

```
# -*- coding: utf-8 -*-
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
import asyncio

@gen.coroutine
def coroutime_visit():
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch("http://www.163.com")
    print(response.body)

@gen.coroutine
def test():
    print('--- start test()')
    yield coroutime_visit()
    print('--- end test()')


loop = asyncio.get_event_loop()
loop.run_until_complete(test())

```


3, 通过协程函数调用协程函数

```
# -*- coding: utf-8 -*-
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

@gen.coroutine
def coroutime_visit():
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch("http://www.163.com")
    print(response.body)

def func_normal():
    print('start to call a coroutine')
    IOLoop.current().run_sync(lambda: coroutime_visit())
    print('end of calling coroutine')

func_normal()

```

4, 在协程中调用阻塞函数

```
# -*- coding: utf-8 -*-

from tornado import gen
from concurrent.futures import ThreadPoolExecutor
import time

thread_pool = ThreadPoolExecutor(2)

def mySleep(count):
    for i in range(count):
        time.sleep(i)
        print('exec mySleep()')

@gen.coroutine
def call_blocking():
    print('---- start ---')
    yield thread_pool.submit(mySleep, 5)
    print('---- end ---')

call_blocking()

```

## 异步化及协程化

Tornado有两种方式可改变同步的处理流程

> 异步化： 针对 RequestHandler的处理函数使用 @tornado.web.asynchronous装饰器，将默认的同步机制改为异步机制
> 协程化： 针对 RequestHandler的处理函数使用 @tornado.gen.coroutine装饰器，将默认的同步机制改为协程机制

1, 异步化

···
import tornado.ioloop
import tornado.web
import tornado.httpclient

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch("http://www.baidu.com",
                   callback=self.on_response)

    def on_response(self, response):
        if response.error: raise tornado.web.HTTPError(500)
        self.write(response.body)
        self.finish()


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

def main():
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
···
本例中用装饰器 tornado.web.asynchronous定义了HTTP访问处理函数 get()，当get()函数返回时，对改HTTP访问的请求尚未完成，所以Tornado无法发送HTTP REsposne.只有当在随后的on_response()中的finish()函数被调用时，Tornado才知道本次处理已经完成，可以发送Response给客户端。

2， 协程化




## 异步调用
```
import tornado.ioloop
import tornado.web
import tornado.httpclient

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch("http://www.baidu.com",
                   callback=self.on_response)
        print('--- get111 ---')

    def on_response(self, response):
        if response.error: raise tornado.web.HTTPError(500)
        self.write(response.body)
        self.finish()
        print('--- on_response ---')

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ], debug=True)

def main():
    app = make_app()
    app.listen(8888)
    print('--- start tornado service')
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
```



## 使用@run_on_executor 创建线程，写异步请求

```
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
``` 
只需要引入这两个包，加上@run_on_executor注解即可  凡是请求当前get的接口都是异步

```
@run_on_executor
def get(self, url=None):
```

## tornado.gen是一个生成器
yield关键字的作用是返回控制，异步任务执行完毕后，程序在yield的地方恢复。

可以看到使用生成器，异步后业务处理不是在回调函数中完成的，看起来像同步处理一样，代码逻辑更清晰。

使用生成器和回调函数异步请求是一样的。


```
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from tornado.httpclient import AsyncHTTPClient, HTTPClient


class SynIndexHandler(tornado.web.RequestHandler):

    def get(self):
        print('invoke SynIndexHandler()')
        client = AsyncHTTPClient()
        response = client.fetch("http://www.163.com")
        self.write("invoke AsyncHTTPClient success")
        self.finish()

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[
                    (r"/syn", SynIndexHandler)
        ] , debug=True)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(5000)
    print('--- http://127.0.0.1:5000 ---')
    tornado.ioloop.IOLoop.instance().start()
```







