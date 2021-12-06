#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import boto3
from boto3.session import Session
import botocore
import json
from time import gmtime, strftime, sleep




class Prophet_data_setting:
    

    def get_json(self,data_dic,data_aws):
        


        self.json = data_dic["input_file"]
        self.aws_access_key = data_aws["aws_access_key_id"]
        self.aws_secret_key = data_aws["aws_secret_access_key"]
        


        self.base = self.json['data_set']['target_file']['base_data']
        
        self.start_date = self.json['data_set']['start_date']
        self.end_date = self.json['data_set']['end_date']
        self.output = self.json['output']
        self.target_file = list(self.json['output'].keys())
        self.target = self.json['data_set']['target_file']
        self.output_dir = self.json['output_path']['dir']
        self.output_bucket = self.json['output_path']['bucket']

        self.upload_check = data_dic["upload_check"]

        self.output_path = 'data/' + self.output_dir + '-' + strftime(data_dic["date_format"], gmtime()) + '/'

        self.aws_ = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
        

            
    def check_data(self):

        if 'data' in os.listdir():
            pass
        else:
            os.mkdir('data')
        

        for d,i in enumerate(self.target.keys()):
            
            if self.target[i]['data_url'].split('/')[2] in os.listdir('data'):
                print('{} exist'.format(self.target[i]['data_url'].split('/')[2]), flush=True)
            
            else:
                print()
                print('{} not exist'.format(self.target[i]['data_url'].split('/')[2]), flush=True)
                print()
                print('download', flush=True)
                print()
                self.aws_.download_file(self.target[i]['Bucket'],self.target[i]['data_url'],'data/'+self.target[i]['data_url'].split('/')[2])
                print('{} download finish'.format(self.target[i]['data_url'].split('/')[2]), flush=True)
                print()

    


    def setting_data(self):
        dt_index = pd.date_range(start=self.start_date, end=self.end_date)
        date_list = dt_index.strftime("%Y-%m-%d").tolist()
        base_data = pd.read_csv('data/'+self.base['data_url'].split('/')[2],encoding=self.base['encoding'])
        
        if self.output_dir in os.listdir():
            pass

        else:
            os.mkdir(self.output_dir)


        for i in self.output.keys():
            
            df = pd.DataFrame()
            df['datetime'] = pd.to_datetime(date_list)
            df['sales'] = base_data.iloc[base_data.loc[base_data[base_data.columns[0]] == i].index.values[0]].values[1:]


            if self.output[i]:

                for j in self.output[i]:

                    col = []
                    data = pd.read_csv('data/'+self.output[i][j]['file'],encoding=self.output[i][j]['encoding'])


                    if self.output[i][j]['type'] == 'int' :
                        data = pd.DataFrame(data.loc[data[self.output[i][j]['key']] == int(self.output[i][j]['values'])])

                    elif self.output[i][j]['type'] == 'float' :
                        data = pd.DataFrame(data.loc[data[self.output[i][j]['key']] == float(self.output[i][j]['values'])])
                    
                    else:
                        data = pd.DataFrame(data.loc[data[self.output[i][j]['key']] == self.output[i][j]['values']])


                    data[self.output[i][j]['on']] = pd.to_datetime(data[self.output[i][j]['on']],format="%Y-%m-%d")

                    
                    col.append(self.output[i][j]['on'])
                    col += self.output[i][j]['use_columns'].copy()


                    df = pd.merge(df,data[col],on = self.output[i][j]['on'], how = 'left').fillna(0)
                    df.to_csv('{}/{}.csv'.format(self.output_dir,i),index=False)

                    if self.upload_check:
                        self.aws_.upload_file(f'{self.output_dir}/{i}.csv',self.output_bucket,f'{self.output_path}{i}.csv')
                        print(f'{i}.csv upload finish', flush=True)


            else:

                df.to_csv('{}/{}.csv'.format(self.output_dir,i),index=False)
                
                if self.upload_check:
                    self.aws_.upload_file(f'{self.output_dir}/{i}.csv',self.output_bucket,f'{self.output_path}{i}.csv')
                    print(f'{i}.csv upload finish', flush=True)

                
                
        print()
        self.json['info']['Bucket'] = self.output_bucket
        self.json['info']['target_file'] = self.target_file
        self.json['info']['data_url'] = self.output_path
        
        
        with open('test.json', 'w') as outfile:
            json.dump(self.json, outfile,indent=4)

        print('Prophet data setting finish', flush=True)

