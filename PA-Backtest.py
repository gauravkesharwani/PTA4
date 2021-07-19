from datetime import datetime
from datetime import timedelta

import pandas as pd
import sqlite3
import yfinance as yf
from progressbar import ProgressBar
import logging

logging.basicConfig(filename='Log\PA-Backtest.log', encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')

con = sqlite3.connect('Data\pairs.db')


def get_spread_const(s1, s2, constDate, info):
    info['diff'] = info[s1] / info[s2]
    info['MA20Day'] = info['diff'].rolling(window=20).mean()
    print(info.loc[str(constDate), 'MA20Day'])
    return round(info.loc[str(constDate), 'MA20Day'], 2)


def back_test(s1, s2):
    constDate = datetime.today().date() - timedelta(days=395)
    startDate = constDate - timedelta(30)
    endDate = datetime.today().date()

    info = yf.download([s1, s2], start=startDate, end=endDate)['Close']

    n = get_spread_const(s1, s2, constDate, info)

    info['Symbol1'] = s1
    info['Symbol2'] = s2
    info['Spread'] = info[s1] - (info[s2] * n)
    info['Constant'] = n
    info['StartDate'] = startDate
    info['EndDate'] = endDate
    info['ConstDate'] = constDate

    info['MA20Day'] = info['Spread'].rolling(window=20).mean()
    info['20 Day STD'] = info['Spread'].rolling(window=20).std()
    info['UpperBand'] = info['MA20Day'] + (info['20 Day STD'] * 2)
    info['LowerBand'] = info['MA20Day'] - (info['20 Day STD'] * 2)

    long = (info.Spread <= info.LowerBand)
    short = (info.Spread >= info.UpperBand)
    info.loc[long, 'Position'] = 'Long'
    info.loc[short, 'Position'] = 'Short'

    closelong = (info.Spread >= info.MA20Day)
    closeshort = (info.Spread <= info.MA20Day)
    info.loc[closelong, 'Close'] = 'Long'
    info.loc[closeshort, 'Close'] = 'Short'

    for index, row in info.iterrows():
        if row['Position'] == 'Long':
            try:
                info1 = info[index:]
                ind = info1.index
                condition = info1['Close'] == "Long"
                CloseDate = ind[condition][0]
                S1ClosePrice = info.loc[CloseDate, s1]
                S2ClosePrice = info.loc[CloseDate, s2]
                S1Shares = round(1000 / row[s1])
                S2Shares = round(1000 / row[s2])
                S1PnL = (S1ClosePrice - row[s1]) * S1Shares
                S2PnL = (row[s2] - S2ClosePrice) * S2Shares
                S1Capital = row[s1] * S1Shares
                S2Capital = row[s2] * S2Shares
                Capital = S1Capital + S2Capital
                Pnl = S1PnL + S2PnL
                PnlPerc = Pnl * 100 / Capital
                delta = CloseDate - index

                info.loc[index, 'CloseDate'] = CloseDate
                info.loc[index, 'S1ClosePrice'] = S1ClosePrice
                info.loc[index, 'S2ClosePrice'] = S2ClosePrice
                info.loc[index, 'S1Shares'] = S1Shares
                info.loc[index, 'S2Shares'] = S2Shares
                info.loc[index, 'S1Capital'] = S1Capital
                info.loc[index, 'S2Capital'] = S2Capital

                info.loc[index, 'S1PnL'] = S1PnL
                info.loc[index, 'S2PnL'] = S2PnL
                info.loc[index, 'PnL'] = Pnl
                info.loc[index, 'PnL%'] = PnlPerc
                info.loc[index, 'Days'] = delta.days
            except:
                pass


        elif row['Position'] == 'Short':
            try:
                info1 = info[index:]
                ind = info1.index
                condition = info1['Close'] == "Short"
                CloseDate = ind[condition][0]
                S1ClosePrice = info.loc[CloseDate, s1]
                S2ClosePrice = info.loc[CloseDate, s2]
                S1Shares = round(1000 / row[s1])
                S2Shares = round(1000 / row[s2])
                S1PnL = (row[s1] - S1ClosePrice) * S1Shares
                S2PnL = (S2ClosePrice - row[s2]) * S2Shares
                S1Capital = row[s1] * S1Shares
                S2Capital = row[s2] * S2Shares
                Capital = S1Capital + S2Capital
                Pnl = S1PnL + S2PnL
                PnlPerc = Pnl * 100 / Capital
                delta = CloseDate - index

                info.loc[index, 'CloseDate'] = CloseDate
                info.loc[index, 'S1ClosePrice'] = S1ClosePrice
                info.loc[index, 'S2ClosePrice'] = S2ClosePrice
                info.loc[index, 'S1Shares'] = S1Shares
                info.loc[index, 'S2Shares'] = S2Shares
                info.loc[index, 'S1Capital'] = S1Capital
                info.loc[index, 'S2Capital'] = S2Capital
                info.loc[index, 'S1PnL'] = S1PnL
                info.loc[index, 'S2PnL'] = S2PnL
                info.loc[index, 'PnL'] = Pnl
                info.loc[index, 'PnL%'] = PnlPerc
                info.loc[index, 'Days'] = delta.days
            except:
                pass

    info = info.dropna()
    # info.to_csv('pt.csv')

    info.rename({s1: 'S1OpenPrice', s2: 'S2OpenPrice'}, axis=1, inplace=True)
    # info.to_sql(name='BackTest4', con=con, if_exists='append')
    info.to_csv('Data\BTResult.csv')

df = pd.read_sql_query(
    "select * from CorrelatedPairs where StartDate='2019-07-15' and [ADF p-value]<=0.05 and [ADF p-value]!=0", con)
df.drop_duplicates(subset=['Symbol1', 'Symbol2'], inplace=True)
print(df)

pbar = ProgressBar()

for index in pbar(df.index[:1]):
    s1 = df['Symbol1'][index]
    s2 = df['Symbol2'][index]
    back_test(s1, s2)
    logging.debug('Backtested Pair #{0} {1}-{2}'.format(index, s1, s2))
