import os

import pandas as pd
from sqlalchemy import create_engine

from config import (MYSQL_DATABASE, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT,
                    MYSQL_USER)

__all_patterns__ = None


DATA_FOLDER = 'data'


def create_mysql_engine():
    mysql_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8"
    engine = create_engine(mysql_url)
    return engine


def csv2sqlite(data_frame, table_name):
    engine = create_mysql_engine()
    print(f'save datafrom to {table_name}')
    print(data_frame)

    data_frame.to_sql(table_name, engine, schema=MYSQL_DATABASE, if_exists='replace', index=False,
                      chunksize=None, dtype=None)


def load_all_datas(data_path=DATA_FOLDER):
    print(f'load data from data path: {data_path}')
    dfs = []
    for file in os.listdir(data_path):
        path = os.path.join(data_path, file)
        code = file.replace('.csv', '')
        df = pd.read_csv(path, index_col=0)
        df['code'] = code
        dfs.append(df)
    return dfs
