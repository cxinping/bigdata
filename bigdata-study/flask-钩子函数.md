* 钩子函数


```

from flask import Flask
import time
import os

app = Flask(__name__)


#before_first_request函数一般用来作一次初始化工作
@app.before_first_request
def before_first_request():
    print("这是before_first_request钩子函数")

@app.before_request
def before_request():
    print("这是before_request钩子函数")

@app.after_request
def after_request(response):
    print("这是after_request钩子函数")
    response.headers["Content-Type"] = "application/json"
    return response

@app.teardown_request
def teardown_request(e):
    print("这是teardown_request钩子函数")

@app.route('/')
def hello_world():
    print("您访问了首页！")
    time.sleep(5)
    return 'Hello World!'

if __name__ == '__main__':
    app.run()
```