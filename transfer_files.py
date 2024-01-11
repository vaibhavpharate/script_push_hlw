import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime, timedelta
import shutil
from configs.paths import *
from configs.database_config import data_configs_local

from database_funcs import get_connection



def get_ssh():
    ssh = paramiko.SSHClient() ## Create the SSH object
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # no known_hosts error
    try:
        ssh.connect(source_ip, username='ubuntu', key_filename=source_key)
    except Exception as e:
        print("There was an error")
        print(e)
    else:
        print("Connected Securely to the Source Server")
    return ssh

def choose_latest_date(ssh_client,source_path,folder_format):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}')
    date_folder_list = stdout.readlines()
    date_folder_list = [str(x)[:-1] for x in date_folder_list]
    date_folder_list.remove("EXIM")
    date_folder_dates  = [datetime.strptime(x,folder_format) for x in date_folder_list]
    date_folder_dates.sort()
    latest_date = date_folder_dates[-1]

    choosing_latest =latest_date.strftime(folder_format)
    return choosing_latest

# ssh_client = get_ssh()
# choose_latest_date(ssh_client=ssh_client,source_path=source_path,folder_format=folder_format)

def seperate_files(files_list,read_variables):
    list_nc_files = []
    for x in range(len(files_list)):
        ext = files_list[x].split('.')[1]
        if ext == 'nc':
            # print(ext)
            var_holder = files_list[x].split('_')[2]
            # print(var_holder)
            if var_holder in read_variables:
                list_nc_files.append(files_list[x])
    return list_nc_files



def transfer_ct_files(ssh_client,variable_files,latest_date,file_timestamp,db_connection):
    sftp_client = ssh_client.open_sftp()
    if os.path.exists(f'{destination_path}/{latest_date}') == False:
        os.mkdir(f'{destination_path}/{latest_date}')

    # create EXIM FILES
    if os.path.exists(f'{destination_path}/EXIM/{latest_date}') == False:
        os.mkdir(f'{destination_path}/EXIM/{latest_date}')

    yesterday = datetime.now().date() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y%m%d")
    lst_folders = list(os.walk(f'{destination_path}'))[0][1]
    for x in lst_folders:
        if x!= latest_date and x!=yesterday and x!='EXIM':
            print(f"Removing older folder {x}")
            shutil.rmtree(f'{destination_path}/{x}', ignore_errors=True)
    for x in variable_files:
        timstmp = datetime.strptime(x[:-3].split('_')[-1],file_timestamp) + timedelta(hours=5,minutes=30)
        variable = 'CT'
        try:
            if os.path.exists(f'{destination_path}/{latest_date}/{x}')==False:
                sftp_client.get(f'{source_path}/{latest_date}/{x}',f'{destination_path}/{latest_date}/{x}')
                df = pd.DataFrame({'timestamp':[timstmp],'variable':[variable],'status':['transferred'],'log_ts':[datetime.now()],'file':[x],'read_status':[0]})
                df.to_sql(schema='file_logs',name='transfer_logs',if_exists='append',con=db_connection,index=False)
            else:
                print(f"File already exists {x}")
        except Exception as e:
            print(e)
    

def transfer_exim_files(ssh_client,exim_file_name,latest_date,exim_path=f'{destination_path}/EXIM'):
    # S_NWC_EXIM-CT_MSG2_IODC-VISIR_20240110T111500Z_030.nc
    
    sftp_client = ssh_client.open_sftp()
    if os.path.exists(f"{exim_path}/{latest_date}/{exim_file_name}") == False:
        sftp_client.get(f'/home/ubuntu/SAFNWC/SAFNWC_Export/EXIM/{latest_date}/{exim_file_name}',f'{exim_path}/{latest_date}/{exim_file_name}')
        print(f"Transferred Exim file {exim_file_name}")
        return True
    else:
        return False


    
