import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
import quandl
import numpy as np
from dateutil.relativedelta import relativedelta
import time, requests, json



def timeis(func):  
    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
          
        print('[{}] is executed in {:.2f} seconds'.format(func.__name__, end-start))
        return result
    return wrap


class BacktestEngine():
    @timeis
    def __init__(self, API_key=None, fred_list=[], yfinance_list=[], market_fred_list=[]):
        
        if API_key is not None:
            self.API_key = API_key
        else:
            self.API_key = 'ad0b46ed99911d1f77534d035a2cdb72'
            
        self.cache = {}
        self.initialize(fred_list, yfinance_list)
        
    def initialize(self, fred_list, yfinance_list):
        def divide_by_ticker(df):
            return {ticker:df[df.ticker.values == ticker] 
                                for ticker in set(df.ticker)}
        
        market_df = self.update_market(market_fred_list)
        macro_df = self.update_macro(fred_list)
        index_df = self.update_index(yfinance_list)


        self.cache['market'] = divide_by_ticker(macro_df)
        self.cache['macro'] = divide_by_ticker(macro_df)
        self.cache['index'] = divide_by_ticker(index_df)
        
    def update_market(self, market_fred_list):

        market_list = ['BAMLH0A0HYM2', 'T10Y2Y','T10YIE', 'T5YIE']
        market_list = set(market_list+market_fred_list)

        df = None
        for ticker in market_list:
            df_add = self._get_PIT_market_df(ticker)
            df_add['ticker'] = [ticker for _ in df_add.index]
            df = pd.concat([df, df_add],axis=0)
            print("--- good ---")

        df = df.sort_index()        
        return df


    def update_macro(self, fred_list):

        macro_list = ['CPIAUCSL', 'PCE', 'M2', 'ICSA']
        macro_list = set(macro_list+fred_list)

        df = None
        for ticker in macro_list:
            df_add = self._get_PIT_df(ticker)
            # df_add = self._get_PIT_df_rev(ticker)
            df_add['ticker'] = [ticker for _ in df_add.index]
            df = pd.concat([df, df_add],axis=0)
            print("--- good ---")

        df = df.sort_index()        
        return df


    def _get_PIT_df(self, ID):
        API_KEY = self.API_key
        REAL_TIME_START, REAL_TIME_END = '2000-01-01', '9999-12-31'
        
        url = 'https://api.stlouisfed.org/fred/series/observations?series_id={}'.format(ID)
        url += '&realtime_start={}&realtime_end={}&api_key={}&file_type=json'.format(
                                        REAL_TIME_START, REAL_TIME_END, API_KEY)
        response = requests.get(url)
        observations = json.loads(response.text)['observations']
        
        df = pd.DataFrame(observations).sort_values(['date','realtime_start']
            ).groupby('date').first()
        df.index = pd.to_datetime(df.index)
        df.realtime_start = pd.to_datetime(df.realtime_start)

        df['datekey'] = df.realtime_start
        df['is_inferred'] = (df.datekey == df.datekey.shift(1))|(
            df.datekey == df.datekey.shift(-1))

        non_inferred_df = df[df['is_inferred']==False]
        lag_list = [(y-x).days for x,y in 
                        zip(non_inferred_df.index, non_inferred_df.datekey)]
        mean_lag, max_lag = int(np.mean(lag_list)+1), int(np.max(lag_list)+1)
        
        df.datekey = [
            date + relativedelta(days=mean_lag) if df.loc[date].is_inferred
            else df.loc[date].datekey
            for date in df.index]

        df = df[['value','datekey','is_inferred']]
        df['cdate'] = df.index
        df = df.set_index('datekey')
        print(df)
        return df

    def _get_PIT_market_df(self, ID):
        API_KEY = self.API_key
        REAL_TIME_START, REAL_TIME_END = '2000-01-01', '9999-12-31'
        
        _start_end_pair = [
            ['2000-01-01', '2004-12-31'],
            ['2005-01-01', '2009-12-31'],
            ['2010-01-01', '2014-12-31'],
            ['2015-01-01', '2019-12-31'],
            ['2020-01-01', '9999-12-31']
        ]

        df0 = None
        for date_range in _start_end_pair:
            REAL_TIME_START = date_range[0]
            REAL_TIME_END = date_range[1]
            
            url = 'https://api.stlouisfed.org/fred/series/observations?series_id={}'.format(ID)
            url += '&observation_start={}&observation_end={}&api_key={}&frequency={}&file_type=json'.format(
                                            REAL_TIME_START, REAL_TIME_END,API_KEY,'d')
            print(url)

            
            response = requests.get(url)
            observations = json.loads(response.text)['observations']
            print(pd.DataFrame(observations).columns)

            
            df = pd.DataFrame(observations)
            df = df[['date','value']].sort_values(['date']).groupby('date').first()
            print(df)
            df.index = pd.to_datetime(df.index)
            df.realtime_start = pd.to_datetime(df.realtime_start)

            df['datekey'] = df.realtime_start
            df['is_inferred'] = (df.datekey == df.datekey.shift(1))|(
                df.datekey == df.datekey.shift(-1))

            non_inferred_df = df[df['is_inferred']==False]
            lag_list = [(y-x).days for x,y in 
                            zip(non_inferred_df.index, non_inferred_df.datekey)]
            mean_lag, max_lag = int(np.mean(lag_list)+1), int(np.max(lag_list)+1)
            
            df.datekey = [
                date + relativedelta(days=mean_lag) if df.loc[date].is_inferred
                else df.loc[date].datekey
                for date in df.index]

            df = df[['value','datekey','is_inferred']]
            df['cdate'] = df.index
            df = df.set_index('datekey')

            print(date_range)
            
            df0 = pd.concat([df0, df], axis=1)

        return df


    def update_index(self, yfinance_list):
        df = None
        # Basic Index
        yf_ticker_list = [
            '^GSPC','^IXIC','^DJI','^RUT','^VIX','^TNX','^SP500TR',
            'GC=F', 'CL=F']
        # Broad Market ETF
        yf_ticker_list += [
            'SPY', # SPDR S&P 500 ETF Trust # 1993-01-22
            'EFA', # iShares MSCI EAFE ETF # 2001-08-14
            'EEM', # iShares MSCI Emerging Markets ETF # 2003-04-07
            'AGG', # iShares Core U.S. Aggregate Bond ETF # 2003-09-22
            ]
        # # Fixed Income ETF
        # yf_ticker_list += [
        #     'SHY', # iShares 1-3 Year Treasury Bond ETF # 2002-07-22
        #     'IEF', # iShares 7-10 Year Treasury Bond ETF # 2002-07-22
        #     'TLH', # iShares 10-20 Year Treasury Bond ETF # 2007-01-05
        #     'TLT', # iShares 20+ Year Treasury Bond ETF # 2002-07-22
        #     'AGG', # iShares Core U.S. Aggregate Bond ETF # 2003-09-22
        #     'LQD', # iShares iBoxx $ Investment Grade Corporate Bond ETF # 2002-07-22
        #     'HYG', # iShares iBoxx $ High Yield Corporate Bond ETF # 2007-04-04
        #     'TIP', # iShares TIPS Bond ETF # 2003-12-04
        #     'MBB', # iShares MBS ETF # 2007-03-13
        #     'EMB', # iShares J.P. Morgan USD Emerging Markets Bond ETF # 2007-12-17
        # ]
        # # Alternative Asset ETF
        # yf_ticker_list += [
        #     'VNQ', # Vanguard Real Estate Index Fund # 2004-09-23
        #     'GLD', # SPDR Gold Shares # 2004-11-18
        #     'IGF', # iShares Global Infrastructure ETF # 2007-12-10
        #     'USO', # United States Oil Fund, LP # 2006-04-10
        #     'UUP', # Invesco DB US Dollar Index Bullish Fund # 2007-07-20
        #     ]
        # yf_ticker_list += [
        #     'XLC', # Communication Services Select Sector SPDR Fund # 2018-06-18
        #     'XLY', # Consumer Discretionary Select Sector SPDR Fund # 1998-12-16
        #     'XLP', # Consumer Staples Select Sector SPDR Fund # 1998-12-16
        #     'XLE', # Energy Select Sector SPDR Fund # 1998-12-16
        #     'XLF', # Financial Select Sector SPDR Fund # 1998-12-16
        #     'XLV', # Health Care Select Sector SPDR Fund # 1998-12-16
        #     'XLI', # Industrial Select Sector SPDR Fund # 1998-12-16
        #     'XLB', # Materials Select Sector SPDR Fund # 1998-12-16
        #     'XLK', # Technology Select Sector SPDR Fund # 1998-12-16
        #     'XLU', # Utilities Select Sector SPDR Fund # 1998-12-16
        #     ]

        # concatenate ticker list (basic and additional)
        yf_ticker_list = set(yf_ticker_list + yfinance_list)

        for ticker in yf_ticker_list:
            df_add = yf.Ticker(ticker).history(period='max')
            df_add.index.rename('date', inplace=True)
            df_add.columns = ['openadj', 'highadj', 'lowadj', 'closeadj', 
                'volume', 'dividends', 'stock splits']
            df_add['ticker'] = [ticker for _ in df_add.index]
            df = pd.concat([df, df_add],axis=0)
            
        df = df.sort_index()
        return df

    def get_universe(self, date, custom_universe):
        if custom_universe is not None:
            universe_list = custom_universe
        else:
            universe_list = []
            for ticker in self.cache['index'].keys():
                if date in self.cache['index'][ticker].index:
                    universe_list.append(ticker)
        return universe_list

    @timeis
    def run_backtest(self, target_generator, sdate, edate, 
                    period='M', transaction_cost=0, custom_universe=None):
        start_T = time.time()
        self.asset = {}
        self.transaction = {}
        
        sdate = mcal.get_calendar('NYSE').valid_days(
            start_date='2000-01-01', end_date=sdate)[-1].strftime('%Y-%m-%d')
        edate = mcal.get_calendar('NYSE').valid_days(
            start_date='2000-01-01', end_date=edate)[-1].strftime('%Y-%m-%d')

        self.bdates = mcal.get_calendar('NYSE').valid_days(
            start_date=sdate, end_date=edate)
        self.bdates = [x.tz_localize(None) for x in self.bdates]

        print('Backtest period: {} -- {}'.format(self.bdates[1], self.bdates[-1]))

        date = self.bdates[0]
        self.asset[date] = {'cash':1}
        universe_list = self.get_universe(date, custom_universe)
        self.delisted_tickers = []

        target_weight = self.compute_target(date, universe_list, target_generator)
        is_rebal = True

        for date in self.bdates[1:]:
            self.update_asset(date)
            self.liquidate_delisted_tickers(date)
            
            if is_rebal:
                self.rebalance_asset(date, target_weight, transaction_cost)
                
            if self.set_rebal_condition(date, 'M'):
                end_T = time.time()
                print('===','date:{}'.format(date),'/',
                      'total_asset:{:.3f}'.format(sum(self.asset[date].values())),'/',
                      'time elapsed:{:.1f}'.format(end_T-start_T),'===',
                      end='\r')

            is_rebal = self.set_rebal_condition(date, period)
               
            if is_rebal:
                universe_list = self.get_universe(date, custom_universe)
                universe_list = list(set(universe_list)-set(self.delisted_tickers))
                self.delisted_tickers = []
                target_weight = self.compute_target(date, universe_list, target_generator)
                
        print('===','date:{}'.format(date),'/',
                      'total_asset:{:.3f}'.format(sum(self.asset[date].values())),'/',
                      'time elapsed:{:.1f}'.format(end_T-start_T),'===')
        self.asset_df = pd.DataFrame(self.asset).T.fillna(0).iloc[1:]
        self.transaction_df = pd.DataFrame(self.transaction).T.fillna(0)

    def update_asset(self, date):
        yesterday = self.bdates[self.bdates.index(date)-1]
        self.asset[date] = {
            ticker : self.asset[yesterday][ticker]*self.get_return(ticker, date)
            for ticker in self.asset[yesterday]}
        self.transaction[date] = {}

    def rebalance_asset(self, date, target_weight, transaction_cost):
        current_asset = self.asset[date].copy()

        total_asset = sum(current_asset.values())
        target_asset = {ticker:total_asset*target_weight[ticker] for ticker in target_weight}
        transaction_asset = {}
        updated_asset = {}

        for ticker in set(target_asset.keys()).union(set(current_asset.keys())):
            target = target_asset[ticker] if ticker in target_asset else 0
            current = current_asset[ticker] if ticker in current_asset else 0
            transaction = target - current

            if transaction > 0 :
                transaction = (1-transaction_cost)*transaction
                updated_asset[ticker] = current + transaction

            elif transaction <= 0:
                if ticker in target_asset.keys():
                    updated_asset[ticker] = current + transaction
                else:
                    pass

            transaction_asset[ticker] = transaction
            
        assert np.abs(1- sum(target_asset.values())/total_asset) < 1e-6

        self.asset[date] = updated_asset
        self.transaction[date] = transaction_asset

    def get_price(self, ticker, date):
        try:
            if ticker == 'cash':
                ticker_price = 1 
            elif ticker in self.cache['index'].keys():
                ticker_price = self.cache['index'][ticker].closeadj.loc[date]
        except:
            print('{} has no price at {}'.format(ticker, date))
            assert False
        return ticker_price

    def get_return(self, ticker, date):
        if ticker == 'cash':
            return 1
        try:
            curr_price = self.cache['index'][ticker]['closeadj'].loc[date]
            last_price = self.cache['index'][ticker]['closeadj'].shift().loc[date]
            return curr_price/last_price
        except:
            print('\n')
            print('{} is delisted at {}'.format(ticker, date) + '\n')
            self.delisted_tickers.append(ticker)
            return 1

    def liquidate_delisted_tickers(self, date):
        if 'cash' not in self.asset[date]: self.asset[date]['cash'] = 0

        for ticker in self.delisted_tickers:
            if ticker in self.asset[date]:
                self.asset[date]['cash'] += self.asset[date][ticker]
                self.asset[date].pop(ticker)

    def set_rebal_condition(self, date, period):
        try:
            tomorrow = self.bdates[self.bdates.index(date)+1]
        except:
            tomorrow = date

        if period == 'D':
            is_rebal = True 
        elif period == 'W':
            is_rebal = (date.weekday() == 0)
        elif period == 'M':
            is_rebal = (tomorrow.month != date.month)
        else:
            is_rebal = False

        return is_rebal

    def compute_target(self, date, universe_list, mpg):
        mpg.date = date
        target_weight = mpg.compute_target(universe_list)

        return target_weight

