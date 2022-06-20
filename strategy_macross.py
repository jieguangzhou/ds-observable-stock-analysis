import pandas as pd
import talib
from tqdm import tqdm
import sys

from utils import csv2sqlite, load_all_datas


def run(data_path):
    datas = load_all_datas(data_path)
    strategy_dfs = []
    for df in tqdm(datas, desc='run strategy'):
        result = analyse_ma_cross(df)
        strategy_dfs.append(result)
    strategy_dfs = pd.concat(strategy_dfs).reset_index(drop=True)
    csv2sqlite(strategy_dfs, 'macross')
    return strategy_dfs


def analyse_ma_cross(df, fast_ma_n=10, slow_ma_n=30):
    fast_ma = talib.SMA(df['close'], timeperiod=fast_ma_n)
    slow_ma = talib.SMA(df['close'], timeperiod=slow_ma_n)

    fast_ma_last_day = fast_ma.shift(1)
    slow_ma_last_day = slow_ma.shift(1)

    golden_cross = 1 * ((fast_ma > slow_ma) &
                        (fast_ma_last_day <= slow_ma_last_day))
    death_cross = 1 * ((fast_ma < fast_ma) &
                       (fast_ma_last_day >= slow_ma_last_day))

    results = pd.DataFrame(
        {'golden_cross': golden_cross, 'death_cross': death_cross, 'code': df['code'], 'date': df.index})

    return results


if __name__ == "__main__":
    data_path = sys.argv[1]
    run(data_path)
