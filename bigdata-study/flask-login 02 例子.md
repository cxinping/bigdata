models.py

```
# -*- coding: utf-8 -*-
from flask_login import UserMixin

class User(UserMixin):
    pass

users = [
    {'id':'tom', 'username': 'tom', 'password': '111'},
    {'id':'mic', 'username': 'mic', 'password': '111'}
]

def query_user(user_id):
    for user in users:
        if user_id == user['id']:
            return user
```

app.py
```
from flask import Flask, request, redirect, url_for, render_template, flash
from flask_login import LoginManager, login_user, logout_user, current_user, login_required
from models import User, query_user

app = Flask(__name__)
app.secret_key = '1234567'

login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'
login_manager.login_message = '请登录'
login_manager.session_protection = "strong"
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    print('--- load_user user_id={}'.format(user_id) )
    if query_user(user_id) is not None:
        curr_user = User()
        curr_user.id = user_id

        return curr_user


@app.route('/')
@login_required
def index():
    return 'Logged in as: %s' % current_user.get_id()


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_id = request.form.get('userid')
        user = query_user(user_id)
        if user is not None and request.form['password'] == user['password']:

            curr_user = User()
            curr_user.id = user_id

            # 通过Flask-Login的login_user方法登录用户
            login_user(curr_user)

            return redirect(url_for('index'))

        flash('错误的用户名和密码，Wrong username or password!')

    # GET 请求
    return render_template('login.html')

@app.route('/home')
@login_required
def home():
    user_id = current_user.get_id()
    print('user_id={}'.format(user_id))
    return render_template('hello.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    #return 'Logged out successfully!'
    return redirect(url_for('login'))


if __name__ == '__main__':
    app.run(debug=True)
```

templates/login.html
```
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Login Sample</title>
</head>
<body>
    <h1>Login</h1>
    {% with messages = get_flashed_messages() %}
        <div>{{ messages[0] }}</div>
    {% endwith %}
    <form action="{{ url_for('login') }}" method="POST">
        <input type="text" name="userid" id="userid" placeholder="用户ID"></input>
        <input type="password" name="password" id="password" placeholder="密码"></input>
        <input type="submit" name="submit"></input>
    </form>
</body>
</html>
```

templates/hello.html
```
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Login Sample</title>
</head>
<body>

{% if current_user.is_authenticated %}
  <h1>Hello {{ current_user.get_id() }}!</h1>
{% endif %}

</body>
</html>
```