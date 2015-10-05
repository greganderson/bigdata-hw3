from flask import Blueprint
main = Blueprint('main', __name__)

import json
import parse

from flask import Flask, request

@main.route("/", methods=["GET"])
def index():
	a = parse.get_top_10('Brazil')
	return str(a)

def create_app(spark_context):
	parse.setup(spark_context)

	app = Flask(__name__)
	app.debug = True
	app.register_blueprint(main)
	return app
