#!/bin/sh
export FLASK_APP=./preprocessing_/index.py
flask run -h 0.0.0.0 -p 50020
