import os
import sys
import argparse
import warnings
from configparser import ConfigParser 
from time import gmtime, strftime
import json
import requests
import random
import string

import boto3





def send_request(args,df):
	# AWS key 것
	boto_default_session = boto3.setup_default_session()
	boto_session = boto3.Session(botocore_session=boto_default_session, region_name="ap-northeast-2")
	credentials = boto_session.get_credentials()

	# RESTful 


	data = {}	  #dict 생성 해당하는 정보들을 담음
	data['aws_access_key_id'] = credentials.access_key 
	data['aws_secret_access_key'] = credentials.secret_key
	data['json'] = df
	data['upload_check'] = args.upload_check
	data['date_format'] = args.date_format
    

	# refer to this
	# https://curl.trillworks.com/#python
	headers = {'Content-Type': 'application/json'}
	url = 'http://' + args.host + ':' + args.port + '/receive'
	response = requests.post(url, headers=headers, data=json.dumps(data))


	if response.status_code == 200:
		print('prophet preprocessing has been successfully processed!')
	elif response.status_code == 404:
		print('Wrong Request Found.')

	# no longer store credentials
	data['aws_access_key_id'] =  ''.join(random.choice(string.digits+string.ascii_letters) for i in range(24))
	data['aws_secret_access_key'] =  ''.join(random.choice(string.digits+string.ascii_letters) for i in range(24))

	# dump this for other processes
	with open(args.json_name + '_prophet.json', 'w') as outfile:
		json.dump(data, outfile)

	# flushing
	data.clear()

if __name__ == '__main__':


	warnings.filterwarnings("ignore", category=FutureWarning)

	parser = argparse.ArgumentParser()
	
	# usage
	# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
	parser.add_argument('--json_name', type=str, default="prepare")
	parser.add_argument('--host', type=str, default='localhost')
	parser.add_argument('--port', type=str, default="50020")
	parser.add_argument('--upload_check', type=bool, default=True)
	parser.add_argument('--date_format', type=str, default="%Y-%m-%d")


    
	
    
	with open("test.json", "r") as json_file:
		test_data = json.load(json_file)
    
    
	args, _ = parser.parse_known_args()
    
    
	send_request(args,test_data)
