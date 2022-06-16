from sqlalchemy import create_engine
import pandas as pd
import os
from talib import abstract
from tqdm import tqdm
from config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE


__all_patterns__ = None


DATA_FOLDER = 'data'


def create_mysql_engine():
    mysql_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8"
    engine = create_engine(mysql_url)
    return engine


def get_all_patterns():
    global __all_patterns__
    if __all_patterns__:
        return __all_patterns__
    funcs = []
    for func_name in abstract.__TA_FUNCTION_NAMES__:
        func = getattr(abstract, func_name)
        group = func.info['group']
        if group == 'Pattern Recognition':
            funcs.append(func)
    __all_patterns__ = funcs
    return funcs


def csv2sqlite(data_frame, table_name):
    engine = create_mysql_engine()
    print(f'save datafrom to {table_name}')
    print(data_frame)

    data_frame.to_sql(table_name, engine, schema=MYSQL_DATABASE, if_exists='replace', index=False,
                      chunksize=None, dtype=None)


def calc_changes(datas):
    change_dfs = []
    for df in tqdm(datas, desc='calc changes'):
        last_close = df['close'].shift(1)
        change = (df['close'] - last_close) / last_close
        change_df = pd.DataFrame(
            {'change': change, 'code': df['code'], 'date': df.index})
        change_dfs.append(change_df.iloc[1:])
    change_dfs = pd.concat(change_dfs).reset_index(drop=True)
    csv2sqlite(change_dfs, 'stock_change')
    return change_dfs


def calc_patterns(datas):
    patterns_dfs = []
    for df in tqdm(datas, desc='calc patterns'):
        pattern_df = analyse_pattern(df)
        patterns_dfs.append(pattern_df)
    patterns_dfs = pd.concat(patterns_dfs).reset_index(drop=True)
    csv2sqlite(patterns_dfs, 'pattern')
    return patterns_dfs


def analyse_pattern(df):
    pattern_results = {}
    for pattern in get_all_patterns():
        pattern_result = pattern(df)
        pattern_results[pattern.info['name']] = pattern_result
    pattern_results = pd.DataFrame(pattern_results)
    pattern_results['code'] = df['code']
    pattern_results['date'] = df.index
    return pattern_results


def get_pattern_name():
    names = []
    for pattern in get_all_patterns():
        names.append(pattern.info['name'])
    name_df = pd.DataFrame({'pattern_name': names})
    csv2sqlite(name_df, 'pattern_name')
    return name_df


def load_all_datas():
    dfs = []
    for file in os.listdir(DATA_FOLDER):
        path = os.path.join(DATA_FOLDER, file)
        code = file.replace('.csv', '')
        df = pd.read_csv(path, index_col=0)
        df['code'] = code
        dfs.append(df)
    return dfs


if __name__ == "__main__":
    datas = load_all_datas()
    get_pattern_name()
    calc_changes(datas)
    calc_patterns(datas)
