https://spacewander.github.io/explore-flask-zh/7-blueprints.html

http://www.bjhee.com/flask-ad6.html

# 创建一个蓝图

比较好的习惯是将蓝图放在一个单独的包里，所以让我们先创建一个”admin”子目录，并创建一个空的__init__.py表示它是一个Python的包。现在我们来编写蓝图，将其存在”admin/admin_module.py”文件里：

```
from flask import Blueprint

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/')
def index(name):
    return '<h1>Hello, this is admin blueprint</h1>'
```

我们创建了蓝图对象”admin_bp”，它使用起来类似于Flask应用的app对象，比如，它可以有自己的路由admin_bp.route()。初始化Blueprint对象的第一个参数admin指定了这个蓝图的名称，第二个参数指定了该蓝图所在的模块名，这里自然是当前文件。

接下来，我们在应用中注册该蓝图。在Flask应用主程序中，使用app.register_blueprint()方法即可：

```
from flask import Flask
from admin.admin_module import admin_bp

app = Flask(__name__)
app.register_blueprint(admin_bp, url_prefix='/admin')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
```


app.register_blueprint()方法的url_prefix指定了这个蓝图的URL前缀。现在，访问http://localhost:5000/admin/ 就可以加载蓝图的index视图了。

你也可以在创建蓝图对象时指定其URL前缀：

> admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

这样注册时就无需指定：

> app.register_blueprint(admin_bp)