templates/base.html



```
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{% block title %}{% endblock %} -我的网站</title>
</head>
<body>
{% block body %}
    这是基类中的内容
{% endblock %}
</body>
</html>
```



templates/index.html

```
{% extends "base.html" %}
{% block title %}网站首页{% endblock %}
{% block body %}
    {{ super() }}
<h4>这是网站首页的内容!</h4>
{% endblock %}
```



templates/product.html

```
{% extends "base.html" %}
{% block title %}产品列表页{% endblock %}
{% block body %}
<h4>这是产品列表页的内容!</h4>
<h4> 取得网页标题的内容：   {{ self.title() }}</h4>
 {{ super() }}
{% endblock %}
```



app.py

```
from flask import Flask,render_template
app = Flask(__name__)

# http://127.0.0.1:5000/
@app.route('/')
def index():
    return render_template('index.html')

#  http://127.0.0.1:5000/product
@app.route('/product')
def product():
    return render_template('product.html')
if __name__ == '__main__':
    app.run(debug=True)

```