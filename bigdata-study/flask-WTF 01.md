参考资料

http://www.pythondoc.com/flask-wtf/

Web应用程序的一个重要方面是为用户提供一个用户界面。HTML提供了一个标签，用于设计一个接口。一个Form 元素，例如文本输入，单选框等可以适当地使用。通过GET或POST方法将用户输入的数据以Http请求消息的形式提交给服务器端脚本。

* 服务器端脚本必须从http请求数据重新创建表单元素。所以实际上，表单元素必须被定义两次 - 一次是HTML，一次是服务器端脚本。

* 使用HTML表单的另一个缺点是很难（如果不是不可能）动态地呈现表单元素。HTML本身无法验证用户的输入。

这就是 WTForms ，一个灵活的表单，渲染和验证库来得方便的地方。Flask-WTF扩展为这个 WTForms 库提供了一个简单的接口。

使用 Flask-WTF ，我们可以在我们的Python脚本中定义表单域并使用HTML模板来呈现它们。也可以将验证应用于 WTF 字段。

让我们看看这个动态生成的HTML如何工作。

首先，需要安装Flask-WTF扩展。

> pip install flask-WTF

已安装的软件包包含一个 Form 类，该类必须用作用户定义表单的父级。

WTforms 包包含各种表单域的定义。下面列出了一些 标准表单字段 。

```
TextField 代表<input type ='text'> HTML表单元素
BooleanField 代表<input type ='checkbox'> HTML表单元素
DecimalField 用小数显示数字的文本字段
IntegerField 用于显示整数的TextField
RadioField 代表<input type ='radio'> HTML表单元素
SelectField 表示选择表单元素
TextAreaField 代表<testarea> html表单元素
PasswordField 代表<input type ='password'> HTML表单元素
SubmitField 表示<input type ='submit'>表单元素
```

可以设计一个包含文本字段的表单 forms.py
```
from flask_wtf import Form
from wtforms import TextField

class ContactForm(Form):
   name = TextField("Name Of Student")
```

除了 'name' 字段之外，还会自动创建一个CSRF令牌的隐藏字段。这是为了防止 跨站请求伪造 攻击。

页面渲染时，这将产生一个等效的HTML脚本，如下所示。

```
<input id = "csrf_token" name = "csrf_token" type = "hidden" />
<label for = "name">Name Of Student</label><br>
<input id = "name" name = "name" type = "text" value = "" />
```

WTForms包也包含验证器类。在验证表单域时非常有用。以下列表显示了常用的验证器。

```
验证器类和描述
DataRequired 检查输入栏是否为空
Email 检查字段中的文本是否遵循电子邮件ID约定
IPAddress 验证输入字段中的IP地址
Length 验证输入字段中字符串的长度是否在给定范围内
NumberRange 在给定范围内的输入字段中验证一个数字
URL 验证输入字段中输入的URL
```
现在我们将以联系表格的形式为 名称 字段应用 'DataRequired' 验证规则。 **

```
name = TextField("Name Of Student",[validators.Required("Please enter your name.")])
```

表单对象的 validate（） 函数验证表单数据，并在验证失败时抛出验证错误。该 错误 消息被发送到模板。在HTML模板中，错误消息是动态呈现的。

```
{% for message in form.name.errors %}
   {{ message }}
{% endfor %}
```

以下示例演示了上面给出的概念。 Contact form 的设计如下 （forms.py） 。

```
from flask_wtf import Form
from wtforms import TextField, IntegerField, TextAreaField, SubmitField, RadioField, SelectField

from wtforms import validators, ValidationError

class ContactForm(Form):
   name = TextField("Name Of Student",[validators.Required("Please enteryour name.")])
   Gender = RadioField('Gender', choices = [('M','Male'),('F','Female')])
   Address = TextAreaField("Address")
   email = TextField("Email",[validators.Required("Please enter your email address."),
      validators.Email("Please enter your email address.")])
   Age = IntegerField("age")
   language = SelectField('Languages', choices = [('cpp', 'C++'),
      ('py', 'Python')])
   submit = SubmitField("Send")
```

验证器应用于 名称 和 电子邮件 字段。

下面给出的是Flask应用程序脚本 （formexample.py） 。

```
from flask import Flask, render_template, request, flash
from forms import ContactForm
app = Flask(__name__)
app.secret_key = 'development key'

# http://127.0.0.1:5000/contact
@app.route('/contact', methods = ['GET', 'POST'])
def contact():
   form = ContactForm()

   if request.method == 'POST':
      if form.validate() == False:
         flash('All fields are required.')
         return render_template('contact.html', form = form)
      else:
         return render_template('success.html')
   elif request.method == 'GET' :
         return render_template('contact.html', form = form)

if __name__ == '__main__':
   app.run(debug = True)

```

模板的脚本 （contact.html） 如下所示 -

```
<html>
   <body>
      <h2 style = "text-align: center;">Contact Form</h2>
      {% for message in form.name.errors %}
         <div>{{ message }}</div>
      {% endfor %}

      {% for message in form.email.errors %}
         <div>{{ message }}</div>
      {% endfor %}

      <form action = "/contact" method = post>
         <fieldset>
            <legend>Contact Form</legend>
            {{ form.hidden_tag() }}

            <div style = font-size:20px; font-weight:bold; margin-left:150px;>
               {{ form.name.label }}<br>
               {{ form.name }}
               <br>

               {{ form.Gender.label }} {{ form.Gender }}
               {{ form.Address.label }}<br>
               {{ form.Address }}
               <br>

               {{ form.email.label }}<br>
               {{ form.email }}
               <br>

               {{ form.Age.label }}<br>
               {{ form.Age }}
               <br>

               {{ form.language.label }}<br>
               {{ form.language }}
               <br>
               {{ form.submit }}
            </div>

         </fieldset>
      </form>

   </body>
</html>
```


success.html

```
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
ok
</body>
</html>
```