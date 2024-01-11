from sqlalchemy import create_engine, text
from configs.database_config import *
import pandas as pd


def get_connection(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    db_connect = create_engine(connection_string)
    try:
        with db_connect.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n\n---------------------Connection Successful")
        return db_connect
    except Exception as e:
        print("\n\n---------------------Connection Failed")
        print(f"{e}")


def get_ci_ct_map(db_connection):
    df = pd.read_sql_query("SELECT * FROM td_satellite.ct_ci_map",db_connection)
    df = df.loc[:,['ct_flag','new_ci']]
    ct_ci_map = {}
    ct_flag = list(df['ct_flag'])
    new_ci = list(df['new_ci'])

    for x in range(len(ct_flag)):
        ct_ci_map[ct_flag[x]] = new_ci[x]
    return ct_ci_map
