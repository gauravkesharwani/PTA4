from datetime import date

import pandas as pd
import matplotlib.pyplot as plt

pair_trades = pd.read_excel('Data/Pair Trades.xlsx', sheet_name='Pair Trades', parse_dates=['Open Date', 'Close Date'])
pair_trades['S1 PnL'] = (pair_trades['S1 Close Price'] - pair_trades['S1 Price']) * pair_trades['S1 Qty']
pair_trades['S1 PnL%'] = (pair_trades['S1 PnL'] * 100) / (pair_trades['S1 Price'] * abs(pair_trades['S1 Qty']))
pair_trades['S2 PnL'] = (pair_trades['S2 Close Price'] - pair_trades['S2 Price']) * pair_trades['S2 Qty']
pair_trades['S2 PnL%'] = (pair_trades['S2 PnL'] * 100) / (pair_trades['S2 Price'] * abs(pair_trades['S2 Qty']))
pair_trades['PnL'] = pair_trades['S1 PnL'] + pair_trades['S2 PnL']
pair_trades['PnL%'] = (pair_trades['PnL'] * 100) / (
        pair_trades['S2 Price'] * abs(pair_trades['S2 Qty']) + pair_trades['S1 Price'] * abs(pair_trades['S1 Qty']))
pair_trades['Open Spread'] = pair_trades['S1 Price'] - pair_trades['S2 Price'] * pair_trades['Const']
pair_trades['Close Spread'] = pair_trades['S1 Close Price'] - pair_trades['S2 Close Price'] * pair_trades['Const']

pair_trades['Days'] = (pair_trades['Close Date'] - pair_trades['Open Date']).dt.days


symbol1_trades = pair_trades[
    ['Open Date', 'Pair', 'Const', 'Position Type', 'S1', 'S1 Qty', 'S1 Price', 'Close Date', 'S1 Close Price',
     'S1 PnL', 'Days']]
symbol2_trades = pair_trades[
    ['Open Date', 'Pair', 'Const', 'Position Type', 'S2', 'S2 Qty', 'S2 Price', 'Close Date', 'S2 Close Price',
     'S2 PnL', 'Days']]

symbol1_trades.columns = ['Open Date', 'Pair', 'Const', 'Position Type', 'Symbol', 'Qty', 'Open Price', 'Close Date',
                          'Close Price',
                          'PnL', 'Days']

symbol2_trades.columns = ['Open Date', 'Pair', 'Const', 'Position Type', 'Symbol', 'Qty', 'Open Price', 'Close Date',
                          'Close Price',
                          'PnL', 'Days']

trades = symbol1_trades.append(symbol2_trades)
sort_trades = trades.sort_values(by='Pair')
trades = sort_trades.sort_values(by='Open Date')
# print(trades)

perf_report = pd.DataFrame(
    columns=['Type', 'Total PnL', 'Avg PnL(%)', 'Avg Days', 'Count', 'Win Count', 'Bating Avg', 'Win Avg PnL(%)',
             'Loss Avg PnL(%)'])

closed_pair_trades = pair_trades[pair_trades['S1 Close Price'].notna()]
print(closed_pair_trades)

closed_pair_trades_perf = closed_pair_trades.agg({'PnL%': ['mean', 'count'], 'Days': 'mean', 'PnL': 'sum'})

print(closed_pair_trades_perf)
win = closed_pair_trades[closed_pair_trades['PnL%'] > 0].agg({'PnL%': ['mean', 'count']})
loss = closed_pair_trades[closed_pair_trades['PnL%'] < 0].agg({'PnL%': ['mean', 'count']})
win_count = win['PnL%']['count']
count = closed_pair_trades_perf['PnL%']['count']
pnl = round(closed_pair_trades_perf['PnL']['sum'], 2)
avg_pnl_percentage = round(closed_pair_trades_perf['PnL%']['mean'], 2)
avg_days = round(closed_pair_trades_perf['Days']['mean'], 2)
batting_avg = round(win_count * 100 / count, 2)
win_avg_pnl_percentage = round(win['PnL%']['mean'], 2)
loss_avg_pnl_percentage = round(loss['PnL%']['mean'], 2)

new_row = {'Type': 'Pair Trades', 'Total PnL': pnl, 'Avg PnL(%)': avg_pnl_percentage, 'Avg Days': avg_days,
           'Count': count,
           'Win Count': win_count, 'Bating Avg': batting_avg, 'Win Avg PnL(%)': win_avg_pnl_percentage,
           'Loss Avg PnL(%)': loss_avg_pnl_percentage}
# append row to the dataframe
perf_report = perf_report.append(new_row, ignore_index=True)

closed_trades = trades[trades['Close Price'].notna()]

closed_trades['PnL'] = (closed_trades['Close Price'] - closed_trades['Open Price']) * closed_trades['Qty']
closed_trades['PnL%'] = (closed_trades['PnL'] * 100) / (closed_trades['Open Price'] * abs(closed_trades['Qty']))
closed_trades['Days'] = (closed_trades['Close Date'] - closed_trades['Open Date']).dt.days

closed_trades_perf = closed_trades.agg({'PnL%': ['mean', 'count'], 'Days': 'mean', 'PnL': 'sum'})
win = closed_trades[closed_trades['PnL%'] > 0].agg({'PnL%': ['mean', 'count']})
loss = closed_trades[closed_trades['PnL%'] < 0].agg({'PnL%': ['mean', 'count']})
win_count = win['PnL%']['count']
count = closed_trades_perf['PnL%']['count']
pnl = round(closed_trades_perf['PnL']['sum'], 2)
avg_pnl_percentage = round(closed_trades_perf['PnL%']['mean'], 2)
avg_days = round(closed_trades_perf['Days']['mean'], 2)
batting_avg = round(win_count * 100 / count, 2)
win_avg_pnl_percentage = round(win['PnL%']['mean'], 2)
loss_avg_pnl_percentage = round(loss['PnL%']['mean'], 2)

new_row = {'Type': 'Trades', 'Total PnL': pnl, 'Avg PnL(%)': avg_pnl_percentage, 'Avg Days': avg_days, 'Count': count,
           'Win Count': win_count, 'Bating Avg': batting_avg, 'Win Avg PnL(%)': win_avg_pnl_percentage,
           'Loss Avg PnL(%)': loss_avg_pnl_percentage}
# append row to the dataframe
perf_report = perf_report.append(new_row, ignore_index=True)

pair_perf = closed_trades.groupby('Pair').agg({'PnL%': ['mean', 'min', 'count'], 'Days': ['mean', 'max']})
pair_perf.columns = ['Avg PnL(%)', 'Max DD', 'Trades', 'Avg Days', 'Max Days']
pair_perf['Avg PnL(%)'] = round(pair_perf['Avg PnL(%)'], 2)
pair_perf['Max DD'] = round(pair_perf['Max DD'], 2)
pair_perf['Trades'] = pair_perf['Trades'] / 2
pair_perf = pair_perf.reset_index()
pair_perf = pair_perf.sort_values('Avg PnL(%)', ascending=False)

print(pair_perf)

profit = closed_trades[['Close Date', 'PnL']]
profit = profit.sort_values('Close Date')
profit = profit.set_index('Close Date')
cumsum = profit.cumsum()

print(cumsum)

cumsum.reset_index().plot(x='Close Date', y='PnL', legend=False)
# plt.show()
plt.savefig('PnL.pdf')

with pd.ExcelWriter('Data/Pair Trades.xlsx') as writer:
    pair_trades.to_excel(writer, sheet_name='Pair Trades', index=False)
    trades.to_excel(writer, sheet_name='Trades', index=False)
    perf_report.to_excel(writer, sheet_name='Performance', index=False)
    pair_perf.to_excel(writer, sheet_name='Pair Performance', index=False)
    cumsum.to_excel(writer, sheet_name='PnL')
