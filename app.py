from flask import Blueprint
from flask import render_template
main = Blueprint('main', __name__)

import json
import parse

from flask import Flask, request

@main.route("/", methods=["GET"])
def index():
	return render_template('index.html')

@main.route("/page/<string:title>", methods=["GET"])
def page(title):
	return parse.get_page(title)

@main.route("/search/<string:term>", methods=["GET"])
def search(term):
	a = parse.get_top_10(term)

	s = '''
<!DOCTYPE html>
<html>
  <body background="/static/bgimage.jpg">
    <div>'''

	for el in a:
		s += '<a href="/page/' + el[0] + '"><p class="padding">' + el[0] + '</p></a>'
	s += '''
    </div>
  </body>
  <style>
    p.padding {
    padding-top: 0px;
    padding-right: 50px;
    padding-bottom: 25px;
    padding-left: 50px;
    }
    div {
    position: fixed;
    height: 80%;
    width: 80%;
    background: rgba(255, 255, 255, 0.75);
    top: 10%;
    left: 10%;
    }
    legend {
    width: 100px;
    height: 100px;
    overflow: hidden;
    size: 10
    }
    title {
    margin-left: 0;
    }
  </style>
</html>'''
	return s
	

def create_app(spark_context):
	parse.setup(spark_context)

	app = Flask(__name__)
	app.debug = True
	app.register_blueprint(main)
	return app
