from flask import Blueprint, redirect, url_for

index_blueprint = Blueprint('index', __name__)


@index_blueprint.route('/')
def index():
    return redirect(url_for('user.login'))
