from datetime import date, timedelta
import pandas as pd
import sqlite3
import statsmodels.regression.linear_model as rg

import numpy as np
from pandas_datareader import data as web
from statsmodels.tsa.stattools import adfuller, coint
# from arch.unitroot import PhillipsPerron
from progressbar import ProgressBar

years = 2
startdate = date.today() - timedelta(days=365 * years)
enddate = date.today()

etflist = pd.read_csv('SymbolGroups.csv')
print(list(etflist.columns))
conx = sqlite3.connect('pairs.db')

pbar = ProgressBar()

for i in pbar(etflist.columns):

    print('Reading {0}'.format(i))
    tickers = list(etflist[i].dropna())

    info = web.DataReader(tickers, data_source='yahoo', start=startdate, end=enddate)['Adj Close']
    # print(info)

    corr = info.corr().unstack()
    corr = corr[corr != 1].drop_duplicates()
    corr = corr[corr >= 0.8]

    result = pd.DataFrame(
        columns=['Symbol1', 'Symbol2', 'ETF', 'StartDate', 'EndDate', 'Corr Coef', 'ADF p-value', 'Ratio p-value'])

    pbar2 = ProgressBar()

    for x in pbar2(corr.index):
        s1 = x[0]
        s2 = x[1]

        try:

            p = coint(info[s1], info[s2])[1]
        except:
            p = 99

        info['Ratio'] = info[s1] / info[s2]

        try:
            r = adfuller(info['Ratio'].dropna())[1]
        except:
            r = 99

        # spread = info[s1] - rg.OLS(info[s1], info[s2]).fit().params[0] * info[s2]

        # try:
        #   pp = PhillipsPerron(spread.dropna(), trend='ct', test_type='rho').pvalue
        # except:
        #   pp = 99

        result.loc[len(result.index)] = [s1, s2, i, startdate, enddate, corr[x], p, r]

    result.to_sql(name='CorrelatedPairs', con=conx, if_exists='append')

# symbols=['OXBR','INSM','NVOS','SGLB','GBOX','LOB','CERC','GTYH','LEXX','SSKN','IPWR','VRME','VVPR','HIBB','HCSG','AMBO','PRU','BHF']


# filter=(result['ADF p-value']<=0.05) & (result['ADF p-value']>0)
# print(df[filter])
result.to_csv('Pair_Analysis.csv'.format())

"""
def stationarity(a, cutoff = 0.05):
  a = np.ravel(a)
  if adfuller(a)[1] < cutoff:
    print('The series is stationary')
    print('p-value = ', adfuller(a)[1])
  else:
    print('The series is NOT stationary')
    print('p-value = ', adfuller(a)[1])

stationarity(info[s1])
stationarity(info[s2])

def cointegration(a, b):
  if coint(a, b)[1] < 0.05:
    print('The series are cointegrated')
    print('p-value = ', coint(a, b)[1])
  else:
    print('The series are NOT cointegrated')
    print('p-value = ', coint(a, b)[1])
cointegration(info[s1], info[s2])
"""
