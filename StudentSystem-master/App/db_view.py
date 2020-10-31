from flask import Blueprint
from App.models import db, User, Grade, Student, Role, Permission

"""
操作数据库页
删除和创建
"""

db_blueprint = Blueprint('db', __name__)


@db_blueprint.route("/create_db/")
def create_db():
    db.create_all()
    return "创建成功"


@db_blueprint.route("/drop_db/")
def drop_db():
    db.drop_all()
    return "删除成功"
