from functools import reduce

import pandas as pd
import sqlite3

con = sqlite3.connect('Data/pairs.db')

pf = pd.read_sql('Select * from performance', con)

all = pf[pf['Type'] == 'All']
all.sort_values(by='Avg PnL(%)', ascending=False, inplace=True)


stochast = pf[pf['Type'] == 'Stochastic All']
stochast.rename(columns={"Avg PnL(%)": "St Avg PnL(%)", "Batting Avg": "St Batting Avg"}, inplace=True)

long = pf[pf['Type'] == 'Long']
long.rename(columns={"Avg PnL(%)": "Long Avg PnL(%)", "Batting Avg": "Long Batting Avg"}, inplace=True)

short = pf[pf['Type'] == 'Short']
short.rename(columns={"Avg PnL(%)": "Short Avg PnL(%)", "Batting Avg": "Short Batting Avg"}, inplace=True)

open = pf[pf['Type'] == 'Open']
open.rename(columns={"Avg PnL(%)": "Open Avg PnL(%)", "Batting Avg": "Open Batting Avg"}, inplace=True)

close = pf[pf['Type'] == 'Close']
close.rename(columns={"Avg PnL(%)": "Close Avg PnL(%)", "Batting Avg": "Close Batting Avg"}, inplace=True)

dfs = [all.head(50), stochast, long, short, open, close]
result = reduce(lambda left, right: pd.merge(left, right, on=['Symbol1', 'Symbol2', 'Constant']), dfs)



# result.to_csv('Data/insight.csv')



insight = result['Avg PnL(%)'] > result['St Avg PnL(%)']
insight2 = result['Batting Avg'] > result['St Batting Avg']
insight3 = result['Long Avg PnL(%)'] > result['Short Avg PnL(%)']
insight4 = result['Long Batting Avg'] > result['Short Batting Avg']
insight5 = result['Open Avg PnL(%)'] > result['Close Avg PnL(%)']
insight6 = result['Open Batting Avg'] > result['Close Batting Avg']


ins1 = (result[insight]['Avg PnL(%)'].count() * 100) / result['Avg PnL(%)'].count()
ins2 = (result[insight2]['Batting Avg'].count() * 100) / result['Batting Avg'].count()

ins4 = (result[insight4]['Long Avg PnL(%)'].count() * 100) / result['Long Avg PnL(%)'].count()
ins3 = (result[insight3]['Long Batting Avg'].count() * 100) / result['Long Batting Avg'].count()

ins5 = (result[insight5]['Open Avg PnL(%)'].count() * 100) / result['Open Avg PnL(%)'].count()
ins6 = (result[insight6]['Open Batting Avg'].count() * 100) / result['Open Batting Avg'].count()


AvgPnl=round(result['Avg PnL(%)'].mean(),2)
StAvgPnl= round(result['St Avg PnL(%)'].mean(),2)
AvgBA=round(result['Batting Avg'].mean(),2)
StAvgBA= round(result['St Batting Avg'].mean(),2)

LongAvgPnl=round(result['Long Avg PnL(%)'].mean(),2)
ShortAvgPnl= round(result['Short Avg PnL(%)'].mean(),2)
LongAvgBA=round(result['Long Batting Avg'].mean(),2)
ShortAvgBA= round(result['Short Batting Avg'].mean(),2)

OpenAvgPnl=round(result['Open Avg PnL(%)'].mean(),2)
CloseAvgPnl= round(result['Close Avg PnL(%)'].mean(),2)
OpenAvgBA=round(result['Open Batting Avg'].mean(),2)
CloseAvgBA= round(result['Close Batting Avg'].mean(),2)

print(f'Avg PnL% {AvgPnl} > Stochastic Avg PnL% {StAvgPnl}: {ins1}%')
print(f'Batting Avg {AvgBA}> Stochastic Batting Avg {StAvgBA}: {ins2}%')
print(f'Long Avg PnL% {LongAvgPnl}> Short Avg PnL% {ShortAvgPnl}: {ins4}%')
print(f'Long Batting Avg {LongAvgBA} > Short Batting Avg {ShortAvgBA}: {ins3}%')
print(f'Open Avg PnL% {OpenAvgPnl}> Close Avg PnL% {CloseAvgPnl}: {ins5}%')
print(f'Open Batting Avg {OpenAvgBA}> Close Batting Avg {CloseAvgBA}: {ins6}%')

# result.to_csv('Data/PerfAnalysis.csv')
