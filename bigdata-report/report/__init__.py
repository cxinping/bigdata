# -*- coding: utf-8 -*-

'''
Created on 2021-08-02

@author: WangShuo
'''

import os
from flask import Flask, jsonify
from http import HTTPStatus
from .views.report import report_bp
from .views.test import test_bp

def create_app(config_object='config.default', config_map=None):
    # Create and configure the app
    app = Flask(__name__, instance_relative_config=True, instance_path='/opt/bmo-lre/config')

    # configure blue print
    app.register_blueprint(report_bp, url_prefix='/report')
    app.register_blueprint(test_bp, url_prefix='/test')

    # Overwrite default config by input parameter
    app.config.from_object(config_object)

    if config_map is not None:
        # load the test config if passed in
        app.config.from_mapping(config_map)

    # Load the configuration from the instance folder which won't be committed to version control
    app.config.from_pyfile('config.py', silent=True)

    app.logger.debug('app.config=%s', app.config)
    
    # Put configurations into os environment


    # Create logger after app initiated to avoid recursive dependency
    app.logger.debug('os.environ=%s', os.environ)

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        result = {'report_service_status': HTTPStatus.OK}
        result['version'] = '1.0'

        #TODO: test email server connection
        
        status = HTTPStatus.INTERNAL_SERVER_ERROR if HTTPStatus.INTERNAL_SERVER_ERROR in result.values() else HTTPStatus.OK
        
        return jsonify(result), status

    return app

def init_app(config_object='config.default'):
    app = create_app(config_object)

    return app

