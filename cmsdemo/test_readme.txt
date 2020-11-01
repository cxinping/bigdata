1，请求地址
http://127.0.0.1:8000/admin/login/

2, 数据库驱动
mysql+pymysql://root:123456@127.0.0.1:3306/springcloud_db01?charset-utf8

3，安装python模块
pip install -i http://pypi.douban.com/simple/ flask_migrate
pip install -i http://pypi.douban.com/simple/ pillow
pip install -i http://pypi.douban.com/simple/ xpinyin

4.数据库迁移请依次执行下面命令：
（1）	执行命令： “python  manager.py db init”，然后回车，进行初始化。
（2）	执行命令： “python  manager.py db migrate”，然后回车，创建迁移脚本。
（3）	执行命令：“python  manager.py db upgrade”，然后回车，升级数据库。
（4）	执行命令：“python  manager.py db upgrade”，然后回车。
（5）	执行命令：“python manager.py create_user -u admin -p 123456 -e 472888778@qq.com”。
（6）	执行命令：“python manager.py create_user -u admin2 -p 123456 -e 222@qq.com”。
  如果第（5）步不成功，请检查表中是否有字段password。



