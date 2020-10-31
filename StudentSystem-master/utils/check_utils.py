from flask import url_for, redirect, session
from functools import wraps


def is_login(func):
    """
    是否登录装饰器
    :param func:被装饰的函数
    :return: 继续函数进行或者登录视图
    """
    @wraps(func)
    def check_login(*args, **kwargs):
        user_id = session.get('user_id')
        if user_id:
            return func(*args, **kwargs)
        else:
            return redirect(url_for('user.login'))

    return check_login
