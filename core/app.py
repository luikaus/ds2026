"""
Placeholder Flask hello world
"""

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello core!\n'
