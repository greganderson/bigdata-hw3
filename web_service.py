#!/usr/bin/env python

from flask import Flask
from flask import request
from flask import render_template
from flask import redirect, url_for
  
app = Flask(__name__)

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
  print request.form['line']
  return redirect(url_for('index'))

@app.route('/quit')
def quit():
  func = request.environ.get('werkzeug.server.shutdown')
  func()
  return "Quitting..."

if __name__ == '__main__':
  app.debug = True
  app.run(host='0.0.0.0')
