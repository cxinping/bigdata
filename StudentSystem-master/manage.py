from flask_script import Manager
from utils.config import create_app
"""
管理启动主接口
"""
app = create_app()
manager = Manager(app)

if __name__ == '__main__':
    manager.run()
