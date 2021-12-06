import os
import sys
import numpy as np
from flask import Flask, jsonify, request, Response, abort
import joblib
import pandas as pd
from io import StringIO
import json

import boto3
from boto3.session import Session
import botocore

from time import sleep,gmtime, strftime
from .data_prophet_setting import *

app = Flask(__name__)


aws_credential_info = {"aws_access_key_id" : None, "aws_secret_access_key" : None}
additional_info  =  {"input_file" : None, "upload_check" : None, "date_format" : None}


def check_Nones(dict_file):
	value = 0
	for key in dict_file:
		if dict_file[key] == None:
			value = -1

	return value


@app.route('/receive', methods=['POST'])
def process_data():
	data = json.loads(request.data)

	aws_credential_info['aws_access_key_id'] = data.get("aws_access_key_id", None)
	aws_credential_info['aws_secret_access_key'] = data.get("aws_secret_access_key", None)

	additional_info['input_file'] = data.get("json", None)
	additional_info['upload_check'] = data.get("upload_check", None)
	additional_info['date_format'] = data.get("date_format", None)

	check_stop_1 = check_Nones(aws_credential_info)
	check_stop_2 = check_Nones(additional_info)

	if check_stop_1 < 0 or check_stop_2 < 0:
		return abort(404)
	else:
        
		pr = Prophet_data_setting()
		pr.get_json(additional_info,aws_credential_info)


		pr.check_data()
		pr.setting_data()

	aws_credential_info.clear()
	additional_info.clear()

	return jsonify("prepare data completed")

	

if __name__ == "__main__":
	app.run()
