import os
import redis
from flask import Flask
from flask_session import Session
from App.index_view import index_blueprint
from App.user_views import user_blueprint
from App.db_view import db_blueprint

from App.models import db

"""
配置App等 在此注册蓝图、数据库等
"""


def create_app():
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    static_dir = os.path.join(BASE_DIR, 'static')
    templates_dir = os.path.join(BASE_DIR, 'templates')

    app = Flask(__name__,
                static_folder=static_dir,
                template_folder=templates_dir)

    app.register_blueprint(blueprint=index_blueprint, url_prefix='/')
    app.register_blueprint(blueprint=user_blueprint, url_prefix='/user')
    app.register_blueprint(blueprint=db_blueprint, url_prefix='/db')

    # 数据库配置
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:123456@127.0.0.1:3306/practiceLibrary'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    # 打印sql
    app.config["SQLALCHEMY_ECHO"] = True

    # #设置session密钥#
    # 使用redis存session
    app.config['SESSION_TYPE'] = 'redis'
    # 连接redis
    app.config['SESSION_REDIS'] = redis.Redis(host='127.0.0.1', port=6379, db=1)
    app.config['SESSION_USE_SIGNER'] = True  # 是否强制加盐，混淆session
    app.config['SECRET_KEY'] = os.urandom(24)  # 如果加盐，那么必须设置的安全码，盐
    app.config['SESSION_PERMANENT'] = False  # sessons是否长期有效，false，则关闭浏览器，session失效
    app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # session长期有效，则设定session生命周期，整数秒，默认大概不到3小时。
    Session(app)

    db.init_app(app=app)

    return app
