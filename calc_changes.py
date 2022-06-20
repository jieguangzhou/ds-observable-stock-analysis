import pandas as pd
import talib
from tqdm import tqdm
import sys

from utils import csv2sqlite, load_all_datas


def run(data_path):
    datas = load_all_datas(data_path)
    strategy_dfs = []
    for df in tqdm(datas, desc='run strategy'):
        result = calc_changes(df)
        strategy_dfs.append(result)
    strategy_dfs = pd.concat(strategy_dfs).reset_index(drop=True)
    csv2sqlite(strategy_dfs, 'stock_change')
    return strategy_dfs


def calc_changes(df):
    last_close = df['close'].shift(1)
    change = (df['close'] - last_close) / last_close
    change_df = pd.DataFrame(
        {'change': change, 'code': df['code'], 'date': df.index})
    return change_df


if __name__ == "__main__":
    data_path = sys.argv[1]
    run(data_path)
