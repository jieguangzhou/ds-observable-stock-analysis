import pandas as pd
from talib import abstract
from tqdm import tqdm
import sys

from utils import csv2sqlite, load_all_datas

__all_patterns__ = None


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


def run(data_path):
    datas = load_all_datas(data_path)
    strategy_dfs = []
    for df in tqdm(datas, desc='run pattern'):
        result = calc_pattern(df)
        strategy_dfs.append(result)
    strategy_dfs = pd.concat(strategy_dfs).reset_index(drop=True)
    csv2sqlite(strategy_dfs, 'pattern')
    return strategy_dfs


def calc_pattern(df):
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


if __name__ == "__main__":
    get_pattern_name()

    data_path = sys.argv[1]
    run(data_path)
