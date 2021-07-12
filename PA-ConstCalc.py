from datetime import datetime
from datetime import timedelta

import pandas as pd
from pandas_datareader import data as web


def get_spread_const(s1, s2, constDate, info):
    info['diff'] = info[s1] / info[s2]
    info['MA20Day'] = info['diff'].rolling(window=20).mean()
    return round(info.loc[str(constDate), 'MA20Day'], 2)


constDate = datetime.today().date() - timedelta(days=5)
startDate = constDate - timedelta(60)
endDate = datetime.today().date()

perf = pd.read_csv('PairPerformance.csv')
perf = perf[perf['Batting Avg'] >= 0.9]
perf = perf.sort_values(by=['Avg PnL(%)'], ascending=False)

i = 0
for index, row in perf[:50].iterrows():
    print(i)
    s1 = row['Symbol1']
    s2 = row['Symbol2']
    info = web.DataReader([s1, s2], data_source='yahoo', start=startDate, end=endDate)['Adj Close']
    n = get_spread_const(s1, s2, constDate, info)
    perf.loc[index, 'N'] = n
    perf.loc[index, 'Spread'] = "{0}-{1}*{2}".format(s1, s2, n)
    perf.loc[index, 'N-Date'] = constDate
    i = i + 1

perf[:50].to_csv('CurrConst2.csv')

# info = web.DataReader([s1, s2], data_source='yahoo', start=startDate, end=endDate)['Adj Close']
#
# n = get_spread_const(s1, s2, constDate, info)
