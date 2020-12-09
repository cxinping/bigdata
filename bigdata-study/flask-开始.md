# Flask



## 第一个例子

```	
# 从flask框架中导入Flask类
from flask import Flask

# 传入__name__初始化一个Flask实例
app = Flask(__name__)
#这个路由将根URL映射到了hello_world函数上
@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    #指定默认主机为是127.0.0.1
    app.run(debug=True  )


```	

## jsonfiy

restful接口

```	
from flask import jsonify

tasks = [
    {
        'id': 1,
        'title': u'订阅 python_mastery 专栏',
        'description': u'专栏Link： https://xiaozhuanlan.com/python_mastery'
    },
    {
        'id': 2,
        'title': u'订阅 pythonml 专栏',
        'description': u'专栏Link： https://xiaozhuanlan.com/pythonml'
    }
]

@users_bp.route('/test', methods=['GET'])
def test():
    return jsonify({'tasks': tasks})
```		
	