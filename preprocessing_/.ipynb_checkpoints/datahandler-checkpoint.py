#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import boto3
import botocore
import subprocess
import numpy as np
import pandas as pd
 
 
from datetime import datetime
from logger import logger
# from errors import AliceFileNotFoundException
from .utils import make_datetime_format,convert_data_split
from boto3.session import Session
 
 
class Repo(object):
    def __init__(self, project, bucket, repo='s3'):
        self.project = project
        
        boto_default_session = boto3.setup_default_session()
        boto_session = boto3.Session(botocore_session=boto_default_session, region_name="ap-northeast-2")
        credentials = boto_session.get_credentials()
        
        self.aws_access_key = credentials.access_key 
        self.aws_secret_key = credentials.secret_key
 
        if repo == 's3':
            self.repo = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
            self.bucket = bucket
            self.date = datetime.now().strftime('%Y%m%d')
 
    def upload_data(self, f: str, bucket=None):
        """
 
        """
        if bucket is None: bucket = self.bucket
        

 
        self.repo.upload_file(f, bucket, f'data/{self.project}/{f}')
        print(f'Upload data complete: data/{self.project}/{f}')
 
 
    def download_data(self, output_path: str, f: str, bucket=None):
        """
 
        """
        if bucket is None: bucket = self.bucket
        

        print(os.listdir())
        if 'data' in os.listdir():

            pass
        else:
            os.mkdir('data')
 
        self.repo.download_file(bucket, f'data/{self.project}/{f}', f'{output_path}/{f}')
        print(f'Download data complete: data/{self.project}/{f}')
 
 
    def upload_outputs(self, source_dir:str, f: str, s3_dir:str, bucket=None):
        """
 
        """
        if bucket is None: bucket = self.bucket
 
        self.repo.upload_file(f'{source_dir}/{f}',bucket ,f'{s3_dir}/{f}')
        print(f'Upload output complete: outputs/{f}')
     
     
    def upload_metrics(self, f: str, bucket=None):
        """
 
            :param f:
            :param bucket:
            :return:
        """
        if bucket is None: bucket = self.bucket
 
        self.repo.upload_file(f, bucket, f'metrics/{self.project}/{self.date}/{f}')
        print(f'Upload metrics complete: metrics/{self.project}/{self.date}/{f}')
 
 
    def download_metircs(self, f: str, bucket=None):
        """
 
        :return:
        """
        if bucket is None: bucket = self.bucket
 
        self.repo.download_file(bucket, f'metrics/{self.project}/{self.date}/{f}', f)
        print(f'Download metrics complete: data/{self.project}/{self.date}/{f}')
 
 
 
def get_basic_files_from_s3(
    s3_dir,
    bucket_name,
    aws_access_key_id=None,
    aws_secret_access_key=None):
 
    session = Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket = session.resource('s3').Bucket(bucket_name)
 
    objs = list(bucket.objects.filter(Prefix=s3_dir, Delimiter='./'))
     
    # if we don't have furthest files, abort
    if len(objs) == 0:
        pass
        # Todo
        #
        # abort(404)
         
    key_list = []
    for i in range(len(objs)):
        key_list.append(objs[i].key)
 
    # get rid of the folder name itself from this list
    return key_list[1:]
     
 
def download_files_from_s3_to_restful(
    s3_file,
    local_file,
    bucket_name,
    aws_access_key_id=None,
    aws_secret_access_key=None):
 
    session = Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
 
    try:
        session.resource('s3').Bucket(
            bucket_name).download_file(s3_file, local_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3_file
            # Todo
            #
            # abort(404)
        else:
            raise
 
 
def upload_files_from_restful_to_s3(
    local_file,
    s3_file,
    bucket_name,
    aws_access_key_id=None,
    aws_secret_access_key=None):
 
    session = Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
 
    try:
        session.resource('s3').Bucket(
            bucket_name).upload_file(local_file, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        local_file
        # Todo
        #
        # abort(404)
 
 
 
def make_tar(f_name: str, source_dir: str):
    subprocess.call(f'tar -czf {f_name} *', cwd=source_dir, shell=True)
 
 
def load_data(f_name: str) -> pd.DataFrame:
    df = pd.read_csv(f_name, dtype={'ds': str, 'y': np.int16})
    return df
 
 
def prepare_data(
    data_name: str,
    data_split: str or float,
    ds_col_name: str,
    y_col_name: str,
    repo: Repo = None,
    local_data_path: str = None,
    data_encoding: str = None):
    """
 
    """
 
    if local_data_path is None:
        if not os.path.exists('data'): os.mkdir('data')   
        repo.download_data('data', data_name)
        df = pd.read_csv(
            f'data/{data_name}',
            encoding=data_encoding)
 
    else:
        if local_data_path.endswith('/'): local_data_path = local_data_path[:-1]
        df = pd.read_csv(
            os.path.join(local_data_path, data_name),
            encoding=data_encoding)
 
    columns = df.columns.tolist()
    ds_col_num = df.columns.get_loc(ds_col_name)
    y_col_num = df.columns.get_loc(y_col_name)
    columns[ds_col_num] = 'ds'
    columns[y_col_num] = 'y'
    df.columns = columns
    
    # df['ds'] = df['ds'].apply(make_datetime_format)
    # df['ds'] = pd.to_datetime(df['ds'])
    # df.sort_values(by=['ds'], inplace=True)
     
    if isinstance(data_split, float):
        split_loc = int(round(len(df) * data_split, 0))
        train_data = df.iloc[:split_loc]
        test_data = df.iloc[split_loc:]
 
    # elif isinstance(data_split, str):
    #     data_split = convert_data_split(data_split)
    #     split_loc = int(round(len(df) * data_split, 0))
    #     train_data = df.iloc[:split_loc]
    #     test_data = df.iloc[split_loc:]



    train_data.reset_index(inplace=True,drop=True)
    test_data.reset_index(inplace=True,drop=True)
 
    return (train_data, test_data)

