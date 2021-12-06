import os
import sys
import numpy as np

from flask import Flask, jsonify, request, Response, abort
from sklearn.decomposition import PCA


import joblib
import pandas as pd
from io import StringIO

import json

import boto3
from boto3.session import Session
import botocore



app = Flask(__name__)


aws_credential_info = {"aws_access_key_id" : None, "aws_secret_access_key" : None, "bucket_name" : None, "region" : None}
additional_info  =  {"output_data_dir" : None, "output_file" : None}
input_file_spec_info = {"csv_encoding" : "default", "input_file_format" : "csv"}


def check_Nones(dict_file):
	value = 0
	for key in dict_file:
		if dict_file[key] == None:
			value = -1

	return value



def upload_files_from_RESTful_to_S3(local_file, s3_file):
	session = Session(aws_access_key_id=aws_credential_info['aws_access_key_id'], aws_secret_access_key=aws_credential_info['aws_secret_access_key'])

	try:
		#s3.upload_file(local_file, bucket, s3_file)
		session.resource('s3').Bucket(aws_credential_info['bucket_name']).upload_file(local_file, s3_file)
		print("Upload Successful")
		return True
	except FileNotFoundError:
		print("The local file was not found")
		return False
	except NoCredentialsError:
		print("Credentials not available")
		return False


@app.route('/receive', methods=['POST'])
def process_data():
	data = json.loads(request.data)

	aws_credential_info['aws_access_key_id'] = data.get("aws_access_key_id", None)
	aws_credential_info['aws_secret_access_key'] = data.get("aws_secret_access_key", None)
	aws_credential_info['bucket_name'] = data.get("bucket_name", None)
	aws_credential_info['region'] = data.get("region", None)

	additional_info['input_file'] = data.get("input_file", None)	
	additional_info['output_model_dir'] = data.get("output_model_dir", None)
	additional_info['output_data_dir'] = data.get("output_data_dir", None)
	additional_info['output_file'] = data.get("output_file", None)	

	input_file_spec_info['csv_encoding'] = data.get("csv_encoding", "default")
	
	# determine which format the input file is 
	splitted_input_str = additional_info['input_file'].split('.')
	input_file_spec_info['input_file_format'] = splitted_input_str[len(splitted_input_str)-1].lower()


	check_stop_1 = check_Nones(aws_credential_info)
	check_stop_2 = check_Nones(additional_info)

	if check_stop_1 < 0 or check_stop_2 < 0:
		return abort(404)
	else:

		# PCA specific
		hyperparameter_info['PCA_component'] = data.get("PCA_component", 5)

		if input_file_spec_info['input_file_format'] == "csv":
			download_files_from_S3_to_RESTful('/tmp/input.csv')

			# read csv data
			if input_file_spec_info['csv_encoding'].lower() == "default":
				raw_data = pd.read_csv('/tmp/input.csv')
			else:		
				raw_data = pd.read_csv('/tmp/input.csv', encoding=input_file_spec_info['csv_encoding'].lower()) 


		elif input_file_spec_info['input_file_format'] == "npy":
			download_files_from_S3_to_RESTful('/tmp/input.npy')

			with open('/tmp/input.npy', 'rb') as f:
				raw_data = np.load(f)
		else:
			jsonify("Input file format must be either csv or npy")

		pca_ = PCA(n_components=hyperparameter_info['PCA_component'])

		if input_file_spec_info['input_file_format'] == "csv":
			fitted_data = pca_.fit_transform(raw_data.values)
		else:
			fitted_data = pca_.fit_transform(raw_data)

		# save the result to tmp
		np.save('/tmp/result.npy', fitted_data)

		s3_file = additional_info['output_data_dir'] + '/' + additional_info['output_file']
		upload_files_from_RESTful_to_S3('/tmp/result.npy', s3_file)

		#flushing
		aws_credential_info.clear()
		additional_info.clear()
		input_file_spec_info.clear()

		return jsonify("Process completed")

	

if __name__ == "__main__":
	app.run()
