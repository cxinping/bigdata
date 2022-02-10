### 引用资料

http://www.pythondoc.com/flask-login/

http://www.bjhee.com/flask-ext8.html

https://www.jianshu.com/p/06bd93e21945

https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-v-user-logins

使用Flask实现用户登陆认证的详细过程
https://www.jianshu.com/p/06bd93e21945


### 使用入门
首先，先概述下例子，有三个url，分别是

> /aut/login    用于登陆

> /auth/logout  用于注册

> /test         用于测试需要登陆后才能访问

### 安装必要的库

> pip install Flask

> pip install Flask-Login

> pip install Flask-WTF

> pip install WTForms

### 编写web框架

首先，在开始登录之前，我们先把整个 web 的框架搭建出来，也就是，我们要能够先在不登录的情况下访问到上面提到的三个url，这个架构比较简单了，我就直接放在一个叫做 app.py 的文件中了。

```
from flask import Flask, Blueprint

app = Flask(__name__)

auth = Blueprint('auth' , __name__)

@auth.route('/login' , methods=['GET', 'POST'])
def login():
    return "login page"

@auth.route('/logout' , methods=['GET', 'POST'])
def logout():
    return "logout page"

@app.route('/test')
def test():
    return "yes, your are allowed"

app.register_blueprint(auth, url_prefix='/auth')

if __name__ == '__main__':
    app.run(debug=True)

```

现在，我们可以尝试一下运行一下这个框架，使用 python app.py 运行即可，然后打开浏览器，分别访问一下，看一下是否都正常

```
http://localhost:5000/test
http://localhost:5000/auth/login
http://localhost:5000/auth/logout
```

### 设置登录才能查看

现在框架已经设置完毕，那么我们就可以尝试一下设置登录需求的，也就是说我们将 test 和 auth/logout 这两个 page 设置成登录之后才能查看。因为这个功能已经和 login 有关系了，所以这时我们就需要使用到 Flask-Login了。我们可以这样来更改代码：

```
from flask import Flask, Blueprint
from flask_login import LoginManager, login_required

app = Flask(__name__)

# 新增代码开始 ===============
app.secret_key = '123456'
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'auth.login'
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    return None

# 新增代码结束 ===============

auth = Blueprint('auth' , __name__)

@auth.route('/login' , methods=['GET', 'POST'])
def login():
    return "login page"

@auth.route('/logout' , methods=['GET', 'POST'])
@login_required
def logout():
    return "logout page"

@app.route('/test')
@login_required
def test():
    return "yes, your are allowed"

app.register_blueprint(auth, url_prefix='/auth')

if __name__ == '__main__':
    app.run(debug=True)
```

其实我们就增加了两项代码，一项是初始化 LoginManager 的， 另外一项就是给test 和 auth.logout添加了login_required 的装饰器，表示要登录了才能访问。

你也许会有疑问：

> @login_manager.user_loader

这个装饰器是干嘛用的。这个在后面的 Question 中有详细得介绍，在这里我们只需要知道这个函数需要返回指定 id 的用户，如果没有就返回 None。这里因为设置框架所以就默认返回 None。

### 用户授权
到此，我们发现访问 test 是不能访问的，会被重定向到 login 的那个 page。那我们看一下我们现在的代码，我们发现 login_required 有了， 那么就差login了，好，接下来就写login，我们来看看Flask-Login的文档，会发现一个叫做login_user的函数，看看它的原型：

> login_user(user, remember=False, duration=None, force=False, fresh=True)

这里需要一个user的对象，所以我们就先创建一个Model，其实，这个Model还是有一点讲究的，所以我们最好是继承自Flask-Login的UserMixin，然后需要实现几个方法，Model 为

```
# user models
class User(UserMixin):
    def is_authenticated(self):
        return True
    
    def is_active(self):
        return True
    
    def is_anonymous(self):
        return False
    
    def get_id(self):
        return "1"
```

这里给所有的函数都返回了默认值，默认对应的情况是这个用户已经登录，并且是有效的。

然后在 login 的 view 里面 login_user， logout的view里面logout_user，这样整个登录过程就连接起来了，最后的代码是这样的：

### 总结

到此，这就是一个比较精简的Flask-Login 教程了，通过这个框架大家可以自行扩展，达到更丰富的功能，后续会连续这个Login 功能继续讲解一下权限控制。

### 问题

#### 未登录访问鉴权页面如何处理：

如果未登录访问了一个做了login_required限制的view，那么flask-login会默认flash一条消息，并且将重定向到log in view， 如果你没有指定log in view， 那么 flask-login将会抛出一个401错误。

#### 如何指定log in view

指定log in view 只需要直接设置login_manager即可：
> login_manager.login_view = 'auth.login'


#### 如何自定义flash消息

如果需要自定义 flash 的消息，那么还是简单设置 login_manager

> login_manager.login_message = u'请登录!'

还可以设置 flash 消息的级别，一般设置成 info 或者 error：

> login_manager.login_message_category = 'info'

#### 自定义未登录处理函数

如果你不想使用默认的规则，那么你也可以自定义未登录情况的处理函数，只需要使用 login_manager 的 unauthorized_handler 装饰器即可。

```
@login_manager.unauthorized_handler
def unauthorized():
    # do stuff
    return tender_template("some template")

```

#### 匿名用户是怎么处理的？有哪些属性？
在 flask-login 中，如果一个匿名用户访问站点，那么 current_user 对象会被设置成一个 AnonymousUserMixin 的对象，AnonymousUserMixin 对象有以下方法和属性：

* is_active and is_authenticated are False
* is_anonymous is True
* get_id() returns None

#### 自定义匿名用户Model

如果你有需求自定义匿名用户的 Model，那么你可以通过设置 login_manager 的 anonymous_user 属性来实现，而赋值的对象只需是可调用对象（class 和 function都行）即可。

> login_manager.anonymous_user = MyAnonymousUser

#### Flask-Login如何加载用户的

当一个请求过来的时候，如果 ctx.user 没有值，那么 flask-login 就会使用 session 中 session['user_id'] 作为参数，调用 login_manager 中使用 user_loader 装饰器设置的 callback 函数加载用户，需要注意的是，如果指定的 user_id 无效，不应该抛出异常，而是应该返回 None。

```
@login_manager.user_loader
def load_user(user_id):
     return User.get(user_id)
```
session['user_id'] 其实是在调用 login_in 函数之后自动设置的。

#### 如何控制Flask-Login的session过期时间

在 Flask-Login 中，如果你不特殊处理的话，session 是在你关闭浏览器之后就失效的。也就是说每次重新打开页面都是需要重新登录的。

如果你需要自己控制 session 的过期时间的话，

* 首先需要设置 login_manager 的 session类型为永久的，
* 然后再设置 session 的过期时间

```
session.permanent = True
app.permanent_session_lifetime = timedelta(minutes=5)
```