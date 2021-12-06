#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# from model import AliceProphet
from .datahandler import Repo, prepare_data, make_tar
from datetime import datetime
from .metrics import mean_absolute_percentage_error, root_mean_squared_error
from sklearn.metrics import mean_squared_error
from .utils import convert_data_split, str2bool
 
import os
import shutil
import warnings
import argparse
import numpy as np
import multiprocessing
import pandas as pd


import sys
from flask import Flask, jsonify, request, Response, abort
import joblib
from io import StringIO
from time import gmtime, strftime


warnings.filterwarnings('ignore')
multiprocessing.set_start_method("fork")
 
 
METRIC_TABLE = {
    'mape': mean_absolute_percentage_error,
    'mse': mean_squared_error,
    'rmse': root_mean_squared_error}
 
PARAM_DICT = {
    'n_changepoints': int,
    'seasonality_mode': str,
    'seasonality_prior_scale': float,
    'holidays_prior_scale': float,
    'changepoint_prior_scale': float,
    'mcmc_samples': int,
    'interval_width': float,
    'uncertainty_samples': int}
 
 
def __get_params():
    return {p_name: p_type(os.getenv(p_name)) for p_name, p_type in PARAM_DICT.items() if os.getenv(p_name) is not None}
 
 
def run_train(
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    project_name: str,
    save_model: bool = False,
    save_plot: bool = False,
    **kwargs):
    """
         
    """
    current_time = datetime.now().strftime("%Y-%m-%d:H%M%S")
    pro_name = project_name.split('/')[0]
    test_id = f'{pro_name}-{current_time}'
    model_path = os.path.join('models', test_id)
    output_path = os.path.join('outputs', test_id)
     
     
    if not os.path.exists('models'): os.mkdir('models')
    if not os.path.exists('outputs'): os.mkdir('outputs')
    os.mkdir(model_path)
    os.mkdir(output_path)
     
     
    # Prophet Model fitting.
    model = AliceProphet(
        output_path=output_path,
        model_path=model_path,
        test_id=test_id,
        **kwargs)
    

    # if len(train_data.columns) >2:
    #     for i in train_data.columns[2:]:
    #         model.add_regressor(i)

    model.add_country_holidays(country_name='KR')


    model.fit(train_data)
    model.set_metrics(**METRIC_TABLE)
    
    y_pred = test_data.drop('y',axis=1)

    test_pred = model.predict(test_data,y_pred)


    # return test_pred,model
     
    # Plotting & save test results.
    model.plot_result(test_pred, save=save_plot)
    model.plot_forecasting(pd.concat([train_data, test_data]), test_pred, save=save_plot)
    model.plot_components(test_pred, save=save_plot)
    model.plot_changepoint(test_pred, save=save_plot)
 
    # # Save model & metrics.
    if save_model: model.save_model()
    model.save_metrics()
     
    return {
        'id': test_id,
        'metrics': model.metrics,
        'result': 'complete',
        'outputs': output_path,
        'models' : model_path}
 
 
def run_prophet(
        data_name: str,
        project_name : str,
        bucket_name : str,
        data_split: str or float,
        ds_col_name: str,
        y_col_name: str,
        local_data_path: str,
        save_model: bool,
        save_plot: bool,
        data_encoding: str = None):
    """
 
    """
    data_split = convert_data_split(data_split)
    upload_output = bool(int(os.getenv('ALICE_UPLOAD_OUTPUT', True)))
    remove_output = bool(int(os.getenv('ALICE_REMOVE_OUTPUT', True)))
    params = __get_params()
 
    repo = Repo(
        project=project_name,
        bucket=bucket_name)
 
    train_data, test_data  = prepare_data(
        data_name=data_name,
        data_split=data_split,
        ds_col_name=ds_col_name,
        y_col_name=y_col_name,
        repo=repo,
        local_data_path=local_data_path,
        data_encoding=data_encoding)
     
    result = run_train(
        train_data=train_data,
        test_data=test_data,
        project_name=project_name,
        save_model=save_model,
        save_plot=save_plot,
        **params)
 
    test_id = result.get('id')
    output_path = result.get('outputs')
    model_path = result.get('models')


    print(output_path)

    if upload_output:
        f_name1 = f'{test_id}_plot_metrics.tar.gz'
        f_name2 = f'{test_id}_model.tar.gz'
        
        make_tar(f_name=f_name1, source_dir=output_path)
        make_tar(f_name=f_name2, source_dir=model_path)
        
        repo.upload_outputs(output_path, f_name1,f'data/{repo.project}/outputs')
        repo.upload_outputs(model_path, f_name2,f'data/{repo.project}/models')


    
    if remove_output: 
        shutil.rmtree('outputs')
        shutil.rmtree('models')

 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-name', type=str, default='V3037_line_class_54-02.csv')
    parser.add_argument('--project_name', type=str, default= 'prophet-data-preprocessing-2021-11-23-05-59-51+0000/output_data')
    parser.add_argument('--data-split', type=str, default='0.8')
    parser.add_argument('--bucket_name', type=str,default='datascience-gsitm-cjh')
    parser.add_argument('--ds-col-name', type=str, default='datetime')
    parser.add_argument('--y-col-name', type=str, default='sales')
    parser.add_argument('--local-data-path', type=str, default=None)
    parser.add_argument('--save-model', type=str2bool, default=True)
    parser.add_argument('--save-plot', type=str2bool, default=True)
    parser.add_argument('--data-encoding', type=str, default=None)
    args = parser.parse_args()
 
    data_name = args.data_name
    data_split = args.data_split
    project_name = args.project_name
    bucket_name = args.bucket_name
    ds_col_name = args.ds_col_name
    y_col_name = args.y_col_name
    local_data_path = args.local_data_path
    data_encoding = args.data_encoding
    save_model = args.save_model
    save_plot = args.save_plot
 
 
    run_prophet(
        data_name=data_name,
        data_split=data_split,
        project_name = project_name,
        bucket_name = bucket_name,
        ds_col_name=ds_col_name,
        y_col_name=y_col_name,
        local_data_path=local_data_path,
        save_model=save_model,
        save_plot=save_plot,
        data_encoding=data_encoding)

