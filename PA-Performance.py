import pandas as pd
import sqlite3
from functools import reduce


def performance(bt):
    perf = bt.groupby(['Symbol1', 'Symbol2', 'Constant']).agg(
        {'PnL%': ['mean', 'min', 'count', 'sum'], 'Days': ['mean', 'max']})
    perf.columns = ['Avg PnL(%)', 'Max DD', 'Count', 'Total PnL(%)', 'Avg Days', 'Max Days']
    perf = perf.reset_index()

    # perf['Avg Days'] = round(perf['Avg Days'])
    # perf['Avg PnL(%)'] = round(perf['Avg PnL(%)'])
    # perf['Max DD'] = round(perf['Max DD'])

    win = bt[bt['PnL%'] > 0].groupby(['Symbol1', 'Symbol2', 'Constant']).agg({'PnL%': ['mean', 'count']})
    win.columns = ['Win Avg PnL(%)', 'Win Total']
    win = win.reset_index()

    loss = bt[bt['PnL%'] < 0].groupby(['Symbol1', 'Symbol2', 'Constant']).agg({'PnL%': ['mean', 'count']})
    loss.columns = ['Loss Avg PnL(%)', 'Loss Total']
    loss = loss.reset_index()

    perf = perf.drop_duplicates(subset=['Symbol1', 'Symbol2'])
    perf.to_csv('Data/perf3.csv')
    win = win.drop_duplicates(subset=['Symbol1', 'Symbol2'])
    perf.to_csv('Data/win3.csv')
    loss = loss.drop_duplicates(subset=['Symbol1', 'Symbol2'])
    perf.to_csv('Data/loss3.csv')
    corr = pd.read_sql('Select * from CorrelatedPairs', con)
    corr = corr.drop_duplicates(subset=['Symbol1', 'Symbol2'])

    dfs = [perf, win, loss]
    result = reduce(lambda left, right: pd.merge(left, right, on=['Symbol1', 'Symbol2', 'Constant'], how='outer'), dfs)
    # result['Win Avg PnL(%)'] = result['Win Avg PnL(%)']
    # result['Loss Avg PnL(%)'] = abs(result['Loss Avg PnL(%)'])
    result['Batting Avg'] = result['Win Total'] / result['Count']
    result['Win Loss Ratio'] = result['Win Avg PnL(%)'] / abs(result['Loss Avg PnL(%)'])

    merged = pd.merge(result, corr, how="inner", on=['Symbol1', 'Symbol2'])

    return merged.round(4)


# result = pd.merge(perf, corr, how='inner', on=['Symbol1', 'Symbol2'])
# result = pd.merge(res, corr, how='inner', on=['Symbol1', 'Symbol2'])

con = sqlite3.connect('Data/pairs.db')

bt = pd.read_sql('Select * from Backtest2', con)

perf_pair_wise = performance(bt)
perf_pair_wise['Type'] = 'All'

bt_long = bt[bt['Position'] == 'Long']
perf_long = performance(bt_long)
perf_long['Type'] = 'Long'

pf = perf_pair_wise.append(perf_long)

bt_short = bt[bt['Position'] == 'Short']
perf_short = performance(bt_short)
perf_short['Type'] = 'Short'

pf = pf.append(perf_short)

bt_open = bt[bt['Position Open Type'] == 'Open']
perf_open_position_type_open = performance(bt_open)
perf_open_position_type_open['Type'] = 'Open'

pf = pf.append(perf_open_position_type_open)

bt_close = bt[bt['Position Open Type'] == 'Close']
perf_open_position_type_close = performance(bt_close)
perf_open_position_type_close['Type'] = 'Close'

pf = pf.append(perf_open_position_type_close)




filt1 = (bt['Position'] == 'Long') & (bt['%K'] < 20) & (bt['%D'] < 20)
filt2 = (bt['Position'] == 'Short') & (bt['%K'] > 80) & (bt['%D'] > 80)
filt = filt1 | filt2
perf_pair_wise_stochostic = performance(bt[filt])
perf_pair_wise_stochostic['Type'] = 'Stochastic All'

pf = pf.append(perf_pair_wise_stochostic)

perf_open_position_type_open_stochostic = performance(bt_open[filt])
perf_open_position_type_open_stochostic['Type'] = 'Stochastic Open'

pf = pf.append(perf_open_position_type_open_stochostic)

perf_open_position_type_close_stochostic = performance(bt_close[filt])
perf_open_position_type_close_stochostic['Type'] = 'Stochastic Close'

pf = pf.append(perf_open_position_type_close_stochostic)

#pf.to_csv('Data\PairPerformance4.csv')

pf.to_sql('Performance', con)
