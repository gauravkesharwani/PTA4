from datetime import datetime
from datetime import timedelta
from statsmodels.tsa.stattools import adfuller, coint

import pandas as pd
import yfinance as yf
from progressbar import ProgressBar


def get_spread_const(s1, s2, constDate, info):
    try:
        info['diff'] = info[s1] / info[s2]
        info['MA20Day'] = info['diff'].rolling(window=20).mean()
        return round(info.loc[str(constDate), 'MA20Day'], 2)
    except:
        return 0


constDate = datetime.today().date() - timedelta(days=5)
startDate = constDate - timedelta(60)
endDate = datetime.today().date()

perf = pd.read_csv('PairPerformance.csv')
perf = perf[perf['Batting Avg'] >= 0.9]
perf.sort_values(by=['Avg PnL(%)'], ascending=False, inplace=True)
perf['Coint'] = 99

pbar2 = ProgressBar()
for ind in pbar2(perf.index):
    s1 = perf['Symbol1'][ind]
    s2 = perf['Symbol2'][ind]
    info = yf.download([s1, s2], start='2019-07-12', end='2021-07-13', progress=False)['Adj Close']
    info.dropna(inplace=True)
    perf.loc[ind, 'Corr'] = info[s1].corr(info[s2])
    print(perf.loc[ind, 'Corr'])
    if perf.loc[ind, 'Corr'] >= 0.8:
        try:
            perf.loc[ind, 'Coint'] = coint(info[s1], info[s2])[1]
        except:
            continue

    if perf.loc[ind, 'Coint'] <= 0.05:
        n = get_spread_const(s1, s2, constDate, info)
        perf.loc[ind, 'N'] = n
        perf.loc[ind, 'Spread'] = "{0}-{1}*{2}".format(s1, s2, n)
        perf.loc[ind, 'N-Date'] = constDate

perf.to_csv('CurrConst3.csv')
