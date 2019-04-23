import os

from flask import Flask, redirect
from flask_restless_swagger import SwagAPIManager as APIManager
from flask_sqlalchemy import SQLAlchemy

from atm.models import ATM


def make_absolute(url):
    url = 'sqlite:///' + os.path.abspath(url)

    return url


def create_app():

    atm = ATM()  # we use this one in order to get the database classes

    with open('db_url.cfg', 'r') as f:
        db_url = f.read()

    app = Flask(__name__)
    app.config['DEBUG'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = make_absolute(db_url)
    db = SQLAlchemy(app)

    # Create the Flask-Restless API manager.
    manager = APIManager(app, flask_sqlalchemy_db=db)

    # Create API endpoints, which will be available at /api/<tablename> by
    # default. Allowed HTTP methods can be specified as well.

    @app.route('/')
    def swagger():
        return redirect('/static/swagger/swagger-ui/index.html')

    manager.create_api(atm.db.Dataset, methods=['GET'])
    manager.create_api(atm.db.Datarun, methods=['GET'])
    manager.create_api(atm.db.Hyperpartition, methods=['GET'])
    manager.create_api(atm.db.Classifier, methods=['GET'])

    return app
