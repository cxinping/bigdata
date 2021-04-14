# Tornado

## 参考资料
```	
http://shouce.jb51.net/tornado/ch1.html#ch1-1

tornado中@tornado.web.asynchronous装饰器使用介绍
http://blog.sina.com.cn/s/blog_172bac9430102x0vl.html

```	




## 安装Tornado

```	
pip3 install tornado
```

## hello

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
