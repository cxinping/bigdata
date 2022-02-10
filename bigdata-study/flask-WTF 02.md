app.py
```
from flask import Flask,flash
from flask import url_for,render_template
# from flask_wtf.csrf import CSRFProtect
#导入定义的BaseLogin
from forms import BaseLogin
import config
app = Flask(__name__)
app.config.from_object(config)
# CSRFProtect(app)
#定义处理函数和路由规则，接收GET和POST请求

@app.route('/login',methods=('POST','GET'))
def baselogin():
    form= BaseLogin()
    #判断是否是验证提交
    if form.validate_on_submit():
        #跳转
        flash(form.name.data+'|'+form.password.data)
        return '表单数据提交成功！'
    else:
        #渲染
        return render_template('login.html',form=form)

@app.route('/')
def hello_world():
    return 'Hello World!'
if __name__ == '__main__':
    app.run(debug=True)

```

config.py
```
#coding:utf8
import os
SECRET_KEY = os.urandom(24)
CSRF_ENABLED = True


```

forms.py
```
# -*- coding:utf-8 -*-
#引入Form基类
from flask_wtf import FlaskForm
#引入Form元素父类
from wtforms import StringField,PasswordField
#引入Form验证父类
from wtforms.validators import DataRequired,Length

#登录表单类,继承与Form类
class BaseLogin(FlaskForm):
    #用户名
    name=StringField('name',validators=[DataRequired(message="用户名不能为空")
        ,Length(6,16,message='长度位于6~16之间')],render_kw={'placeholder':'输入用户名'})
    #密码
    password=PasswordField('password',validators=[DataRequired(message="密码不能为空")
        ,Length(6,16,message='长度位于6~16之间')],render_kw={'placeholder':'输入密码'})
```

templates/login.html

```
  <!DOCTYPE html>
   <html lang="en">
   <head>
       <meta charset="UTF-8">
      <title>Flask_WTF</title>
   <style type="text/css">
  .div1 {
        height:180px;
        width:380px;
       border:1px solid #8A8989;
        margin:0 auto;
          }
 .input{
      display: block;
  	width: 350px;
  	height: 40px;
   	margin: 10px auto;
   }
   .button
   	{
       background: #2066C5;
   	color: white;
   	font-size: 18px;
  	font-weight: bold;
   	height: 50px;
  	border-radius: 4px;
   	}
       </style>
   </head>
   <body>
   <div class="div1"><form action="login" method = "post">
   <!--启动CSRF-->
            {{form.hidden_tag()}}
       {{form.name(size=16,id='name',class='input' )}}
       {%for e in form.name.errors%}
                <span style="color: red">{{e}}</span>
                {%endfor%}
  	{{form.password(size=16,id='password',class='input')}}
   {%for e in form.password.errors%}
                <span style="color: red">{{e}}</span>
                {%endfor%}
       <input type="submit" value="登录"  class="input button">
   </form></div>

   </body>
  </html>


```