在Flask中处理文件上传非常简单。它需要一个enctype属性设置为'multipart / form- data'的HTML表单，将该文件发布到URL。URL处理程序从 request.files [] 对象中提取文件并将其保存到所需的位置。

每个上传的文件首先保存在服务器上的临时位置，然后再保存到最终位置。目标文件的名称可以是硬编码的，也可以从 request.files [file] 对象的filename属性中获取。但是，建议使用 secure_filename（） 函数获取它的安全版本。

可以在Flask对象的配置设置中定义默认上传文件夹的路径和上传文件的最大大小。

|  代码   | 说明  |
|  ----  | ----  |
| app.config[‘UPLOAD_FOLDER’]  | 定义上传文件夹的路径 |
| app.config [ 'MAX_CONTENT_PATH']  | 指定要上传的文件的大小 - 以字节为单位 |


templates/upload.html
```
<html>
   <body>
      <form action = "/uploader" method = "POST"
         enctype = "multipart/form-data">
         <input type = "file" name = "file" />
         <input type = "submit"/>
      </form>

   </body>
</html>
```

fileuploadapp.py
```
from flask import Flask, redirect, url_for, request, render_template,abort
from werkzeug.utils import secure_filename

app = Flask(__name__)

# http://127.0.0.1:5000/upload
@app.route('/upload')
def upload_file():
   return render_template('upload.html')

@app.route('/uploader', methods = ['GET', 'POST'])
def uploader_file():
   if request.method == 'POST':
      f = request.files['file']
      filename = secure_filename(f.filename)
      print('filename={}'.format(filename))
      f.save(filename)
      return 'file uploaded successfully'

if __name__ == '__main__':
   app.run(debug = True)
```

访问 http://127.0.0.1:5000/uploader