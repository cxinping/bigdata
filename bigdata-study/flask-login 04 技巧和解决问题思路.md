1,How to override the html default “Please fill out this field” when validation fails in Flask?

https://stackoverflow.com/questions/50787904/how-to-override-the-html-default-please-fill-out-this-field-when-validation-fa

> {{ form.name(required=False) }}


2, 关于Flask-Login中session失效时间的处理

https://www.cnblogs.com/practice-h/p/8883487.html

3, flask 实现 Authorization请求报头认证

flask-login 整合 pyjwt + json 简易flask框架

https://www.cnblogs.com/minsons/p/8047331.html


获得当前时间

nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

返回JSON字符串和状态码

```
from flask import Flask, render_template, redirect, url_for, request,jsonify

@app.route("/testjson")
def testreturn():
    return jsonify([{ 'name': 'wangwu', 'age':21}]), 201
```

Token-Based Authentication With Flask

https://realpython.com/token-based-authentication-with-flask/