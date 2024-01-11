from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import netCDF4 as nc
import os
from sqlalchemy import text
from configs.paths import destination_path, destination_ip,directory_path

from transfer_files import transfer_exim_files
from database_funcs import get_ci_ct_map


def get_transferred_logs(db_connection):
    df = pd.read_sql_query("SELECT * FROM file_logs.transfer_logs WHERE read_status = '0'",con=db_connection)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['minutes_to_subtract'] = 5.5*60
    df['minutes_to_subtract'] = pd.to_timedelta(df['minutes_to_subtract'],'m')
    df['tds'] = df['timestamp'] - df['minutes_to_subtract']
    df['date'] = pd.to_datetime(df['tds'].dt.date)
    df['file_path'] = destination_path +"/"+df['date'].dt.strftime('%Y%m%d') + "/" + df['file']
    return df

def check_if_data_exists(timestamp,db_connection,exim=False):
    if exim == False:
        df = pd.read_sql_query(f"SELECT COUNT(*) FROM haleware.satellite_data WHERE timestamp = '{timestamp}' and disp = 'frv'"
                            ,db_connection)
        print(df['count'][0])
        if df['count'][0] > 0:
            return False
        else:
            return True
    else:
        df = pd.read_sql_query(f"SELECT COUNT(*) FROM haleware.satellite_data WHERE timestamp = '{timestamp}' and disp = 'erv'"
                            ,db_connection)
        print(df['count'][0])
        if df['count'][0] > 0:
            return False
        else:
            return True
    

def get_us_timestamp(tim_stmp):
    us_time = datetime.strptime(tim_stmp,'%Y-%m-%d %H:%M:%S') - timedelta(hours=5,minutes=30)
    us_time = us_time.strftime("%Y%m%dT%H%M00Z")
    return us_time


def date_from_exim(add_time,exim_file_path,variable_atts,db_connection,timestamp,data_map_config):
    org_timestamp = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
    tmstp_str = org_timestamp + timedelta(minutes=add_time)
    tmstp_str = tmstp_str.strftime(format='%Y-%m-%d %H:%M:%S')
    ct_data={1: 'Cloud-free_land',
            5: 'Very_low_clouds',
            6: 'Low_clouds',
            7: 'Mid-level_clouds',
            8: 'High_opaque_clouds',
            9: 'Very_high_opaque_clouds',
            10: 'Fractional_clouds',
            11: 'High_semitransparent_thin_clouds',
            12: 'High_semitransparent_moderately_thick_clouds',
            13: 'High_semitransparent_thick_clouds',
            14: 'High_semitransparent_above_low_or_medium_clouds',
            2: 'Cloud-free_sea',
            3: 'Snow_over_land',
            4: 'Sea_ice'}
    if os.path.exists(exim_file_path):
        data = nc.Dataset(exim_file_path,)
        df = pd.DataFrame()
        df['lat'] = np.array(data.variables['lat'][:]).flatten()
        df['lon'] = np.array(data.variables['lon'][:]).flatten()
        df['timestamp'] = tmstp_str
        ci_ct_map = get_ci_ct_map(db_connection=data_map_config)
            
        for i in variable_atts['CT']:
            df[i] = np.array(data.variables[i][:]).flatten()
        df = df.loc[df['ct']!=255,:]
        try:
            df['ct_flag'] = df['ct'].apply(lambda x: ct_data[x])
            df['ci_data'] = df['ct_flag'].apply(lambda x:ci_ct_map[x])
            df['disp'] = 'erv'
        except Exception as e:
            print(e)
        if check_if_data_exists(timestamp=tmstp_str,db_connection=db_connection,exim=True):
            df.to_sql(name='satellite_data',schema='haleware',if_exists='append',index=False,con=db_connection)
            print(f"Entererd EXIM data for {tmstp_str}")    
        print(df)


def data_to_database(timestamp,file_path,db_connection,variable_atts,ssh_client,latest_date,data_map_config):
    exim_file = 'S_NWC_EXIM-CT_MSG2_IODC-VISIR_'
    file_timestmap = timestamp
    usd_timestamp = timestamp - timedelta(hours=5,minutes=60)
    # latest_date = timestamp.date().strftime("%Y%m%d")
    tmstp_str = str(timestamp) ## This is in IST
    previous_24 = file_timestmap - timedelta(hours=24)
    previous_24 = previous_24.strftime(format='%Y-%m-%d %H:%M:%S')
    minuts = (datetime.now() - file_timestmap)
    minuts = minuts.seconds /60
    # print(f"THIS IS A TIMEDELTA {minuts.seconds/60}")
    ct_data={1: 'Cloud-free_land',
            5: 'Very_low_clouds',
            6: 'Low_clouds',
            7: 'Mid-level_clouds',
            8: 'High_opaque_clouds',
            9: 'Very_high_opaque_clouds',
            10: 'Fractional_clouds',
            11: 'High_semitransparent_thin_clouds',
            12: 'High_semitransparent_moderately_thick_clouds',
            13: 'High_semitransparent_thick_clouds',
            14: 'High_semitransparent_above_low_or_medium_clouds',
            2: 'Cloud-free_sea',
            3: 'Snow_over_land',
            4: 'Sea_ice'}
    try:
        with db_connection.connect() as conn:
            conn.execute(text(f"DELETE FROM haleware.satellite_data  WHERE timestamp <='{previous_24}'"))
            conn.commit()
            conn.execute(text('rollback'))
            conn.execute(text(f"DELETE FROM haleware.satellite_data  WHERE timestamp <='{tmstp_str}' and disp='erv'"))
            conn.commit()
            conn.execute(text('rollback'))
            print(f"Deleted Data Before and including timestamp {previous_24}")
            conn.close()

    except Exception as e:
        print(e)

    if os.path.exists(file_path):
            data = nc.Dataset(file_path)
            df = pd.DataFrame()
            df['lat'] = np.array(data.variables['lat'][:]).flatten()
            df['lon'] = np.array(data.variables['lon'][:]).flatten()
            df['timestamp'] = tmstp_str
            ci_ct_map = get_ci_ct_map(db_connection=data_map_config)
            
            for i in variable_atts['CT']:
                df[i] = np.array(data.variables[i][:]).flatten()
            
            try:
                df['ct_flag'] = df['ct'].apply(lambda x: ct_data[x])
                df['ci_data'] = df['ct_flag'].apply(lambda x:ci_ct_map[x])
                df['disp'] = 'frv'
                # print(df)

                if check_if_data_exists(tmstp_str,db_connection=db_connection):
                    df.to_sql(name='satellite_data',schema='haleware',if_exists='append',index=False,con=db_connection)
                    with db_connection.connect() as conn:
                        print("Update Sheet")
                        conn.execute(text(f"UPDATE file_logs.transfer_logs SET read_status=1 where timestamp = '{tmstp_str}' AND variable='CT'"))
                        conn.commit()
                        # conn.execute(text("rollback"))
                        conn.close()
            except Exception as e:
                print(e)
    exim_file_timestamp = exim_file+usd_timestamp.strftime(format='%Y%m%dT%H%M%SZ')
    # date_folder = None
    print(minuts)
    if int(minuts)<=30 and int(minuts) >=4:
        add_time = 15
        file_1 = exim_file_timestamp + '_015.nc'
        file_path_1 = f"{destination_path}/EXIM/{latest_date}/{file_1}"
        # print(f)
        transfer_exim_files(ssh_client=ssh_client,exim_file_name=file_1,latest_date=latest_date)
        date_from_exim(add_time=add_time,
                       exim_file_path=file_path_1,
                       variable_atts=variable_atts,
                       db_connection=db_connection,
                       timestamp=tmstp_str,
                       data_map_config=data_map_config)
    if int(minuts) > 30 and int(minuts)<45:
        
        file_11 = exim_file_timestamp + '_015.nc'
        transfer_exim_files(ssh_client=ssh_client,exim_file_name=file_11,latest_date=latest_date)
        add_time = 15
        file_path_11 = f"{destination_path}/EXIM/{latest_date}/{file_11}"
        date_from_exim(add_time=add_time,
                       exim_file_path=file_path_11,
                       variable_atts=variable_atts,
                       db_connection=db_connection,
                       timestamp=tmstp_str,
                       data_map_config=data_map_config)
        file_2 = exim_file_timestamp + '_030.nc'
        transfer_exim_files(ssh_client=ssh_client,exim_file_name=file_2,latest_date=latest_date)
        add_time = 30
        file_path_2 = f"{destination_path}/EXIM/{latest_date}/{file_2}"
        date_from_exim(add_time=add_time,
                       exim_file_path=file_path_2,
                       variable_atts=variable_atts,
                       db_connection=db_connection,
                       timestamp=tmstp_str,
                       data_map_config=data_map_config)
        
    
