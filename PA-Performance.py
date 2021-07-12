import pandas as pd
import sqlite3
from functools import reduce

con = sqlite3.connect('pairs.db')

bt = pd.read_sql('Select * from BacktestFinal where  date>"2020-06-01" and Closedate<="2021-06-01"', con)

perf = bt.groupby(['Symbol1', 'Symbol2', 'Constant']).agg({'PnL%': ['mean', 'min', 'count', 'sum'], 'Days': ['mean', 'max']})
perf.columns = ['Avg PnL(%)', 'Max DD', 'Count', 'Total PnL(%)', 'Avg Days', 'Max Days']
perf = perf.reset_index()
perf['CI'] = round((100 * (pow((1 + perf['Avg PnL(%)'] / 100), perf['Count']))) - 100, 2)
perf['SI'] = round((100 * (1 + (perf['Avg PnL(%)'] / 100) * perf['Count'])) - 100, 2)
perf['Sharpe ratio'] = perf['Total PnL(%)']

perf['Avg Days'] = round(perf['Avg Days'])
perf['Avg PnL(%)'] = round(perf['Avg PnL(%)'])
perf['Max DD'] = round(perf['Max DD'])

win = bt[bt['PnL%'] > 0].groupby(['Symbol1', 'Symbol2', 'Constant']).agg({'PnL%': ['mean', 'count']})
win.columns = ['Win Avg PnL(%)', 'Win Total']
win = win.reset_index()

loss = bt[bt['PnL%'] < 0].groupby(['Symbol1', 'Symbol2', 'Constant']).agg({'PnL%': ['mean', 'count']})
loss.columns = ['Loss Avg PnL(%)', 'Loss Total']
loss = loss.reset_index()

perf = perf.drop_duplicates(subset=['Symbol1', 'Symbol2'])
win = win.drop_duplicates(subset=['Symbol1', 'Symbol2'])
loss = loss.drop_duplicates(subset=['Symbol1', 'Symbol2'])
corr = pd.read_sql('Select * from CorrelatedPairs', con)
corr = corr.drop_duplicates(subset=['Symbol1', 'Symbol2'])

dfs = [perf, win, loss, corr]
result = reduce(lambda left, right: pd.merge(left, right, on=['Symbol1', 'Symbol2']), dfs)
result['Win Avg PnL(%)'] = round(result['Win Avg PnL(%)'], 2)
result['Loss Avg PnL(%)'] = round(abs(result['Loss Avg PnL(%)']), 2)
result['Batting Avg'] = round(result['Win Total'] / result['Count'], 2)
result['Win Loss Ratio'] = round(result['Win Avg PnL(%)'] / abs(result['Loss Avg PnL(%)']), 2)


# result = pd.merge(perf, corr, how='inner', on=['Symbol1', 'Symbol2'])
# result = pd.merge(res, corr, how='inner', on=['Symbol1', 'Symbol2'])

result.to_csv('PairPerformance.csv')
