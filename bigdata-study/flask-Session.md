app.secret_key = 'some secret key'

```
# -*- coding: utf-8 -*-

from flask import Flask,session
from datetime import timedelta
import os
app = Flask(__name__)
# 设置session
app.config['SECRET_KEY'] = os.urandom(24)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7) # 配置7天有效

# http://127.0.0.1:5000/
@app.route('/')
def set_session():
    session['username']='zhangsan'
    session['age'] = 21
    session.permanent = True
    return 'Session设置成功!'

# http://127.0.0.1:5000/get_session
#获取session
@app.route('/get_session')
def get_session():
    username=session.get('username')
    return username or 'Session为空！'

#获取session
@app.route('/test')
def test():
    username = session.get('username')
    print('test username=', username )
    if username in session :
        print('username=>' , username)
    else:
        print('username is none ! ')
    return username or 'Session为空！'

#清除sessin
@app.route('/del_session')
def del_session():
    session.pop('username')
    #session.clear #清空Session
    return  'Session被删除！'

if __name__ == '__main__':
    app.run()
```