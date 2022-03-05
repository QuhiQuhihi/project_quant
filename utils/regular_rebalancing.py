import numpy as np
import pandas as pd

def weekly_rebalance_strategy(strategy, yld_df, bm_yld_df,lookback_period = 4):
    """
    :param strategy: investment strategy, this function should return weight of portfolio in list form
    :param yld_df: yield data of asset classes, used as input of strategy generation
    :param bm_yld_df: benchmark yield data of strategy.
    :param lookback_period:
    :return: return of investment strategy and return of benchmark in dataframe
    """
    # How much data to lookback to produce portfolio
    yld_df = yld_df.resample('1W').first()
    bm_yld_df = bm_yld_df.resample('1W').first()

    strategy_weight = []
    for i in range(len(yld_df)):
        if i < lookback_period:
            strategy_weight.append("")
        else:
            strategy_weight.append(strategy(yld_df.iloc[i-lookback_period:i-1]).tolist())

    yld_df['strategy_weight'] = strategy_weight
    yld_df = yld_df.iloc[lookback_period:]

    strategy_return = []
    ret = 0
    for i in range(len(yld_df)):
        # investment return of asset class * weight of asset class
        for j in range(yld_df.shape[1]-1):
            ret += (yld_df.iloc[i,j]) * ((yld_df['strategy_weight'].iloc[i])[j])

        strategy_return.append(ret)
        ret = 0

    yld_df['strategy_return'] = strategy_return
    yld_df['benchmark_return'] = bm_yld_df
    yld_df = yld_df.iloc[lookback_period:]

    return yld_df[['strategy_return', 'benchmark_return']]

def rebalance_strategy(strategy, yld_df, bm_yld_df,lookback_period = 12):
    """
    :param strategy: investment strategy, this function should return weight of portfolio in list form
    :param yld_df: yield data of asset classes, used as input of strategy generation
    :param bm_yld_df: benchmark yield data of strategy.
    :param lookback_period:
    :return: return of investment strategy and return of benchmark in dataframe
    """

    asset_list = yld_df.columns
    date_list = yld_df.index

    investment_weight = pd.DataFrame(columns=asset_list, index=date_list)
    investment_return = pd.DataFrame(columns=['strategy_return','benchmark_return'], index=date_list)

    for i in range(len(investment_return)):
        if i < lookback_period:
            investment_weight.iloc[i,:]= 0
        else:
            investment_weight.iloc[i,:] = strategy(yld_df.iloc[i-lookback_period : i-1])

    investment_weight['SUM']=investment_weight.sum(axis=1)

    for date in (date_list):
        ret = 0
        for asset in asset_list:
            asset_return = yld_df.loc[date, asset]
            asset_weight = investment_weight.loc[date, asset]
            ret += (1 + asset_return) * asset_weight
        investment_return.loc[date,'strategy_return'] = ret - 1


    investment_return.loc[:, 'benchmark_return'] = bm_yld_df
    investment_return = pd.concat([investment_return,investment_weight], axis=1)
    investment_return = investment_return.iloc[lookback_period:]

    return investment_return

def rp_rebalance_strategy(strategy, yld_df, bm_yld_df,lookback_period = 12):
    """
    :param strategy: investment strategy, this function should return weight of portfolio in list form
    :param yld_df: yield data of asset classes, used as input of strategy generation
    :param bm_yld_df: benchmark yield data of strategy.
    :param lookback_period:
    :return: return of investment strategy and return of benchmark in dataframe
    """

    asset_list = yld_df.columns
    date_list = yld_df.index

    investment_weight = pd.DataFrame(columns=asset_list, index=date_list)
    investment_return = pd.DataFrame(columns=['strategy_return','benchmark_return'], index=date_list)

    for i in range(len(investment_return)):
        if i < lookback_period:
            investment_weight.iloc[i,:]= 0
        else:
            investment_weight.iloc[i,:] = strategy(pd.DataFrame(yld_df.iloc[i-lookback_period : i-1]).cov())

    investment_weight['SUM']=investment_weight.sum(axis=1)

    for date in (date_list):
        ret = 0
        for asset in asset_list:
            asset_return = yld_df.loc[date, asset]
            asset_weight = investment_weight.loc[date, asset]
            ret += (1 + asset_return) * asset_weight
        investment_return.loc[date,'strategy_return'] = ret - 1


    investment_return.loc[:, 'benchmark_return'] = bm_yld_df
    investment_return = pd.concat([investment_return,investment_weight], axis=1)
    investment_return = investment_return.iloc[lookback_period:]

    return investment_return

def mv_rebalance_strategy(strategy, yld_df, bm_yld_df,lookback_period = 252):
    """
    :param strategy: investment strategy, this function should return weight of portfolio in list form
    :param yld_df: yield data of asset classes, used as input of strategy generation
    :param bm_yld_df: benchmark yield data of strategy.
    :param lookback_period:
    :return: return of investment strategy and return of benchmark in dataframe
    """

    asset_list = yld_df.columns
    date_list = yld_df.index

    investment_weight = pd.DataFrame(columns=asset_list, index=date_list)
    investment_return = pd.DataFrame(columns=['strategy_return','benchmark_return'], index=date_list)

    for i in range(len(investment_return)):
        if i < lookback_period:
            investment_weight.iloc[i,:]= 0
        else:
            investment_weight.iloc[i,:] = strategy

    investment_weight['SUM']=investment_weight.sum(axis=1)

    for date in (date_list):
        ret = 0
        for asset in asset_list:
            asset_return = yld_df.loc[date, asset]
            asset_weight = investment_weight.loc[date, asset]
            ret += (1 + asset_return) * asset_weight
        investment_return.loc[date,'strategy_return'] = ret - 1


    investment_return.loc[:, 'benchmark_return'] = bm_yld_df
    investment_return = pd.concat([investment_return,investment_weight], axis=1)
    investment_return = investment_return.iloc[lookback_period:]

    return investment_return