# Flask



## 安装Flask

```	
pip3 install flask
```



## werkzeug简介

安装flask后自动就有werkzeug
Werkzeug是一个WSGI工具包，他可以作为一个Web框架的底层库。这里稍微说一下， werkzeug 不是一个web服务器，也不是一个web框架，而是一个工具包，官方的介绍说是一个 WSGI 工具包，它可以作为一个 Web 框架的底层库，因为它封装好了很多 Web 框架的东西，例如 Request，Response 等等 

```	
from werkzeug.wrappers import Response,Request

@Request.application
def hello(request):       #wsgi协议需要两个参数，env和response，这里只有一个参数，不符合该协议,     #werkzeug是个工具包，在wsgi基础上又做了封装，所以传一个参数就行
 
   def hello(request):   
        return Response('Hello World!')

    if __name__ == '__main__':
        from werkzeug.serving import run_simple
        run_simple('localhost', 4000, hello)   #三个参数分别是跑的地址，跑的端口，最后一个是可调用对象
```



## 第一个例子

```	
# 从flask框架中导入Flask类
from flask import Flask

# 传入__name__初始化一个Flask实例
app = Flask(__name__)
#这个路由将根URL映射到了hello_world函数上
@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    #指定默认主机为是127.0.0.1
    app.run(debug=True  )


```



## jsonfiy

restful接口

```	
from flask import jsonify

tasks = [
    {
        'id': 1,
        'title': u'订阅 python_mastery 专栏',
        'description': u'专栏Link： https://xiaozhuanlan.com/python_mastery'
    },
    {
        'id': 2,
        'title': u'订阅 pythonml 专栏',
        'description': u'专栏Link： https://xiaozhuanlan.com/pythonml'
    }
]

@users_bp.route('/test', methods=['GET'])
def test():
    return jsonify({'tasks': tasks})
```



Flask run启动Falsk APP
https://flask.palletsprojects.com/en/1.1.x/quickstart/

$ export FLASK_APP=hello.py
$ flask run

C:\path\to\app>set FLASK_APP=hello.py

Alternatively you can use python -m flask:

$ export FLASK_APP=hello.py
$ python -m flask run

 * Running on http://127.0.0.1:5000/



