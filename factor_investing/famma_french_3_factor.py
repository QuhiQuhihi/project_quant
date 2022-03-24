import pandas as pd
import datetime as dt

from dcodata.api.data import PeriodicData
from dcodata.api.data import HistoricalData

from io import BytesIO
from io import StringIO

from zipfile import ZipFile
from urllib.request import urlopen

from itertools import product

FF3_URL = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip'  # noqa E501
FRED_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS1MO&cosd={}&coed={}'  # noqa E501

FF3_VALID_DATE = '1994-01-01'
FINANCIAL_DATA_LAG = 5

START_DATE = '2018-01-01'
END_DATE = dt.datetime.today().strftime('%Y-%m-%d')


def get_fama_french_data():
    # Fama-French 3 Factor (MKT-RF, SMB, HML, RF)
    ff3_raw = urlopen(FF3_URL).read()
    zip_file = ZipFile(BytesIO(ff3_raw))
    ff3_file_name = 'F-F_Research_Data_Factors_daily.CSV'
    ff3_file = zip_file.open(ff3_file_name)
    df_ff3 = pd.read_csv(ff3_file, index_col=0, skiprows=4).iloc[:-1]

    df_ff3 = df_ff3.reset_index()
    df_ff3.columns = ['date'] + list(df_ff3.columns)[1:]
    df_ff3['date'] = pd.to_datetime(df_ff3['date'].astype(str)).astype(str)

    return df_ff3


def get_rf(start_date, end_date):
    # get 1-mon US treasury bond rate from fred
    fred_url = FRED_URL.format(start_date, end_date)
    rf_raw = urlopen(fred_url).read()
    rf_str = StringIO(rf_raw.decode('utf-8'))
    df_rf = pd.read_csv(rf_str)

    # calculate rf
    df_rf.columns = ['date', 'bond_yld']
    df_rf['bond_yld'] = df_rf['bond_yld'].replace({'.': None})
    df_rf['bond_yld'] = df_rf['bond_yld'].ffill().astype(float)
    df_rf['RF'] = df_rf['bond_yld'] / 365
    df_rf = df_rf[['date', 'RF']]
    RF = df_rf.set_index('date')['RF']

    return RF


def get_current_tot_com_eq(end_date):
    fin_cols = ['ticker', 'year', 'quarter', 'filingdate', 'tot_com_eq']
    start_date = str(int(FF3_VALID_DATE[:4]) - 2) + FF3_VALID_DATE[4:]
    fin = PeriodicData(preset='us_equity_q',
                       start_date=start_date,
                       columns=fin_cols, snapshot='all').get()
    fin = fin.dropna()

    fin = fin.sort_values(['ticker', 'filingdate', 'year', 'quarter'])
    fin = fin.groupby(['ticker', 'filingdate']).tail(1)

    qt_start = {1: '-01-01', 2: '-04-01', 3: '-07-01', 4: '-10-01'}
    fin['q_start_dt'] = fin['year'].astype(str) + \
                        fin['quarter'].replace(qt_start)

    fin['prev_q_start_dt'] = fin.groupby('ticker')['q_start_dt'].shift(1)
    drop_idx = fin[fin['q_start_dt'] < fin['prev_q_start_dt']].index

    fin = fin.drop(drop_idx)

    # get ack_date to consider delay of obtaining data
    fin['ack_date'] = pd.to_datetime(fin['filingdate']) + \
                      pd.Timedelta(days=FINANCIAL_DATA_LAG)
    fin['ack_date'] = fin['ack_date'].astype(str)
    fin = fin[['ticker', 'ack_date', 'tot_com_eq']]
    fin = fin.sort_values(['ticker', 'ack_date'])
    fin = fin.reset_index(drop=True)

    # to ffill, get all dates
    date_series = pd.Series(pd.date_range(
        fin['ack_date'].min(), end_date).astype(str))
    ticker_series = fin['ticker'].drop_duplicates()
    ticker_date_df = pd.DataFrame(
        list(product(ticker_series, date_series)),
        columns=['ticker', 'ack_date'])

    min_by_ticker = fin.groupby('ticker')['ack_date'].min()
    min_by_ticker = min_by_ticker.reset_index()
    min_by_ticker.columns = ['ticker', 'min_date']

    ticker_date_df = pd.merge(
        ticker_date_df, min_by_ticker, how='outer', on='ticker')

    ticker_date_df = ticker_date_df.loc[
        ticker_date_df['ack_date'] >= ticker_date_df['min_date']]
    ticker_date_df = ticker_date_df[['ticker', 'ack_date']]

    fin = pd.merge(fin, ticker_date_df,
                   on=['ticker', 'ack_date'], how='right')

    fin = fin.sort_values(['ticker', 'ack_date'])
    fin['tot_com_eq'] = fin.groupby('ticker')['tot_com_eq'].ffill()
    fin = fin.reset_index(drop=True)

    fin.columns = ['ticker', 'date', 'tot_com_eq']

    return fin


def divide_market_by_size_and_book(df):
    df['book_to_market'] = df['tot_com_eq'] / df['marketcap']

    mc5 = df.groupby('date')['marketcap'].median().reset_index()
    b3 = df.groupby('date')['book_to_market'].quantile(.3).reset_index()
    b7 = df.groupby('date')['book_to_market'].quantile(.7).reset_index()

    mc5.columns = ['date', 'mc5']
    b3.columns = ['date', 'b3']
    b7.columns = ['date', 'b7']

    df = pd.merge(df, mc5, on='date')
    df = pd.merge(df, b3, on='date')
    df = pd.merge(df, b7, on='date')

    B = df[df['marketcap'] > df['mc5']]
    S = df[df['marketcap'] < df['mc5']]

    BV = B[B['book_to_market'] > B['b7']]
    BN = B[(B['book_to_market'] < B['b7']) &
           (B['book_to_market'] > B['b3'])]
    BG = B[B['book_to_market'] < B['b3']]

    SV = S[S['book_to_market'] > S['b7']]
    SN = S[(S['book_to_market'] < S['b7']) &
           (S['book_to_market'] > S['b3'])]
    SG = S[S['book_to_market'] < S['b3']]

    BV = BV.groupby('date')['yld'].mean()
    BN = BN.groupby('date')['yld'].mean()
    BG = BG.groupby('date')['yld'].mean()
    SV = SV.groupby('date')['yld'].mean()
    SN = SN.groupby('date')['yld'].mean()
    SG = SG.groupby('date')['yld'].mean()

    return BV, BN, BG, SV, SN, SG


def calculate_ff3_manually(start_date, end_date):
    mkt_cols = ['date', 'ticker', 'yld', 'marketcap']
    mkt = HistoricalData(preset='us_equity',
                         start_date=start_date,
                         end_date=end_date,
                         columns=mkt_cols).get()
    mkt = mkt.dropna()

    fin = get_current_tot_com_eq(end_date)

    mkt = pd.merge(mkt, fin, on=['ticker', 'date'], how='inner')
    del fin

    # if tot_com_eq is negative, remove it
    mkt = mkt[mkt['tot_com_eq'] > 0]

    BV, BN, BG, SV, SN, SG = divide_market_by_size_and_book(mkt)

    # calculate factors
    SMB = ((SV + SN + SG) / 3 - (BV + BN + BG) / 3) * 100
    HML = ((BV + SV) / 2 - (BG + SG) / 2) * 100

    RF = get_rf(mkt['date'].min(), mkt['date'].max())

    avg_yld_by_date = mkt.groupby('date')['yld'].mean()
    MKT_RF = ((avg_yld_by_date - 1) * 100 - RF).dropna()

    MKT_RF = MKT_RF.reset_index()
    SMB = SMB.reset_index()
    HML = HML.reset_index()
    RF = RF.reset_index()

    MKT_RF.columns = ['date', 'Mkt-RF']
    SMB.columns = ['date', 'SMB']
    HML.columns = ['date', 'HML']
    RF.columns = ['date', 'RF']

    ff3 = pd.merge(MKT_RF, SMB, on='date')
    ff3 = pd.merge(ff3, HML, on='date')
    ff3 = pd.merge(ff3, RF, on='date')

    return ff3


if __name__ == '__main__':
    ff3 = get_fama_french_data()
    ff3 = ff3[ff3['date'] <= FF3_VALID_DATE]

    manual_ff3 = calculate_ff3_manually(START_DATE, END_DATE)
    manual_ff3 = manual_ff3[manual_ff3['date'] > FF3_VALID_DATE]

    ff3 = ff3.append(manual_ff3, ignore_index=True)