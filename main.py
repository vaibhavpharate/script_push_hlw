from database_funcs import get_connection
from configs.database_config import data_configs_local, data_configs_map
from transfer_files import *
from read_files import *


folder_format = "%Y%m%d"
file_timestamp = folder_format+"T%H%M00Z"
# read_variables = ['CMA','CT','CMIC','CRR','CRRPh','CTTH']
read_variables = ['CT',]
db_connection = get_connection(host=data_configs_local['host'],
                               passord=data_configs_local['password'],
                               user=data_configs_local['user'],
                               database=data_configs_local['database'],
                               port=data_configs_local['port'])


variable_atts = {'CTTH':['ctth_pres', 'ctth_alti', 'ctth_tempe', 'ctth_effectiv', 'ctth_method', 'ctth_status_flag'],
 'CMA':['cma_cloudsnow', 'cma', 'cma_dust', 'cma_volcanic', 'cma_smoke', 'cma_testlist1', 'cma_testlist2', 'cma_status_flag', 'cma_conditions'],
 'CRRPh':['crrph_intensity', 'crrph_accum', 'crrph_status_flag', 'crrph_conditions'],
 'CMIC':['cmic_phase', 'cmic_reff', 'cmic_cot', 'cmic_lwp', 'cmic_iwp', 'cmic_status_flag', 'cmic_conditions'],
 'CRR':['crr', 'crr_intensity', 'crr_accum', 'crr_status_flag', 'crr_conditions'],
 'CT':['ct']
}


# 1
ssh_client = get_ssh()
latest_date = choose_latest_date(ssh_client=ssh_client,source_path=source_path,folder_format=folder_format)
stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}/{latest_date}')
variable_folders = stdout.readlines()
variable_folders = [str(x)[:-1] for x in variable_folders]
variable_files = seperate_files(variable_folders,read_variables=read_variables)


transfer_ct_files(ssh_client=ssh_client,
                  variable_files=variable_files,
                  latest_date=latest_date,
                  db_connection=db_connection,
                  file_timestamp=file_timestamp)

tracker_df = get_transferred_logs(db_connection=db_connection)



db_conn_map = get_connection(host=data_configs_map['host'],
                               passord=data_configs_map['password'],
                               user=data_configs_map['user'],
                               database=data_configs_map['database'],
                               port=data_configs_map['port'])

for index, row in tracker_df.iterrows():
    data_to_database(timestamp=row['timestamp'],
                 file_path=row['file_path'],
                 db_connection=db_connection,
                 variable_atts=variable_atts,
                 ssh_client=ssh_client,
                 latest_date=latest_date,
                 data_map_config = db_conn_map)
