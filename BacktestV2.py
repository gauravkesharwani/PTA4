from datetime import datetime
from datetime import timedelta

import pandas as pd
import sqlite3
import yfinance as yf
from progressbar import ProgressBar
import logging

pd.set_option('display.max_rows', None)

logging.basicConfig(filename='Log\PA-Backtest.log', encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')

con = sqlite3.connect('Data\pairs.db')


def get_spread_const(constDate, spread, s1_df, s2_df):
    spread['diff'] = s1_df['Adj Close'] / s2_df['Adj Close']
    spread['MA20Day'] = spread['diff'].rolling(window=20).mean()
    return round(spread.loc[str(constDate), 'MA20Day'], 2)


def get_close_date(opendate, spread, position):
    spread1 = spread[opendate:]
    ind = spread1.index

    condition = spread1.Open >= spread1.MA20Day if position == 'Long' else spread1.Open <= spread1.MA20Day

    CloseDateOpen = ind[condition].min()

    condition = spread1.Close >= spread1.MA20Day if position == 'Long' else spread1.Close <= spread1.MA20Day
    CloseDateClose = ind[condition].min()

    if CloseDateOpen <= CloseDateClose:
        return CloseDateOpen
    else:
        return CloseDateClose


def get_close_price(spread, CloseDate, position, symbol):
    if position == 'Long':
        if spread.loc[CloseDate, 'Open'] >= spread.loc[CloseDate, 'MA20Day']:
            return spread.loc[CloseDate, '{0} Open'.format(symbol)]
        else:
            return spread.loc[CloseDate, '{0} Close'.format(symbol)]
    else:
        if spread.loc[CloseDate, 'Open'] <= spread.loc[CloseDate, 'MA20Day']:
            return spread.loc[CloseDate, '{0} Open'.format(symbol)]
        else:
            return spread.loc[CloseDate, '{0} Close'.format(symbol)]


def get_positions(spread, position, position_open_type):
    positions = spread[spread['Position'] == position]
    for index, row in positions.iterrows():

        try:
            CloseDate = get_close_date(index, spread, position)

            S1ClosePrice = get_close_price(spread, CloseDate, position, 'S1')

            S2ClosePrice = get_close_price(spread, CloseDate, position, 'S2')

            S1OpenPrice = row[f'S1 {position_open_type}']

            S2OpenPrice = row[f'S2 {position_open_type}']

            spread.loc[index, 'Position Open Type'] = position_open_type

            spread.loc[index, 'CloseDate'] = CloseDate

            spread.loc[index, 'S1OpenPrice'] = S1OpenPrice
            spread.loc[index, 'S2OpenPrice'] = S2OpenPrice

            spread.loc[index, 'S1ClosePrice'] = S1ClosePrice
            spread.loc[index, 'S2ClosePrice'] = S2ClosePrice

            # Check
            S1Shares = round(1000 / row[f'S1 {position_open_type}'])
            S2Shares = round(1000 / row[f'S2 {position_open_type}'])

            S1PnL = (S1ClosePrice - S1OpenPrice) * S1Shares if position == 'Long' else (
                                                                                               S1OpenPrice - S1ClosePrice) * S1Shares
            S2PnL = (S2OpenPrice - S2ClosePrice) * S2Shares if position == 'Long' else (
                                                                                               S2ClosePrice - S2OpenPrice) * S2Shares

            S1Capital = row[f'S1 {position_open_type}'] * S1Shares
            S2Capital = row[f'S2 {position_open_type}'] * S2Shares
            # check

            Capital = S1Capital + S2Capital
            Pnl = S1PnL + S2PnL
            PnlPerc = Pnl * 100 / Capital
            delta = CloseDate - index

            spread.loc[index, 'S1Shares'] = S1Shares
            spread.loc[index, 'S2Shares'] = S2Shares
            spread.loc[index, 'S1Capital'] = S1Capital
            spread.loc[index, 'S2Capital'] = S2Capital

            spread.loc[index, 'S1PnL'] = S1PnL
            spread.loc[index, 'S2PnL'] = S2PnL
            spread.loc[index, 'PnL'] = Pnl
            spread.loc[index, 'PnL%'] = PnlPerc
            spread.loc[index, 'Days'] = delta.days

        except:
            pass


def backtest(spread, position_open_type):
    # check
    spread['MA20Day'] = spread[position_open_type].rolling(window=20).mean()
    spread['20 Day STD'] = spread[position_open_type].rolling(window=20).std()
    # check

    spread['UpperBand'] = spread['MA20Day'] + (spread['20 Day STD'] * 2)
    spread['LowerBand'] = spread['MA20Day'] - (spread['20 Day STD'] * 2)

    # check
    long = (spread[position_open_type] <= spread.LowerBand)
    short = (spread[position_open_type] >= spread.UpperBand)
    # check

    spread.loc[long, 'Position'] = 'Long'
    spread.loc[short, 'Position'] = 'Short'

    get_positions(spread, 'Long', position_open_type)
    get_positions(spread, 'Short', position_open_type)


def back_test(s1, s2, s1_df, s2_df, position_open_type):
    spread = pd.DataFrame(columns=['Open', 'Close'])

    n = get_spread_const(constDate, spread, s1_df, s2_df)

    spread['S1 Open'] = s1_df['Open']
    spread['S1 Close'] = s1_df['Close']
    spread['S2 Open'] = s2_df['Open']
    spread['S2 Close'] = s2_df['Close']

    spread['Open'] = s1_df['Open'] - s2_df['Open'] * n
    spread['Close'] = s1_df['Close'] - s2_df['Close'] * n

    spread['High'] = s1_df['High'] - s2_df['High'] * n
    spread['Low'] = s1_df['Low'] - s2_df['Low'] * n

    spread['14-high'] = spread['High'].rolling(14).max()
    spread['14-low'] = spread['Low'].rolling(14).min()
    spread['%K'] = (spread['Close'] - spread['14-low']) * 100 / (spread['14-high'] - spread['14-low'])
    spread['%D'] = spread['%K'].rolling(3).mean()

    spread['Symbol1'] = s1
    spread['Symbol2'] = s2
    spread['Constant'] = n
    spread['StartDate'] = startDate
    spread['EndDate'] = endDate
    spread['ConstDate'] = constDate

    backtest(spread, position_open_type)

    spread.dropna(inplace=True)
    # spread.to_csv(f'Data\BTResult_{position_open_type}.csv')

    spread.to_sql(name='BackTest', con=con, if_exists='append')


df = pd.read_sql_query(
    "select distinct cp.Symbol1, cp.Symbol2 from CorrelatedPairs cp \
left join performance pf on cp.symbol1=pf.Symbol1 and cp.Symbol2=pf.Symbol2 \
where cp.[ADF p-value]<=0.05 and cp.[ADF p-value]!=0 \
and pf.Symbol1 is null", con)

print(df.count())
# df.drop_duplicates(subset=['Symbol1', 'Symbol2'], inplace=True)

pbar = ProgressBar()

for index in pbar(df.index):
    try:

        s1 = df['Symbol1'][index]
        s2 = df['Symbol2'][index]
        print(f'backtesting {s1} {s2}')
        constDate = datetime.today().date() - timedelta(days=395)
        startDate = constDate - timedelta(30)
        endDate = datetime.today().date()

        s1_df = yf.download(s1, start=startDate, end=endDate, progress=False)
        s2_df = yf.download(s2, start=startDate, end=endDate, progress=False)

        back_test(s1, s2, s1_df, s2_df, 'Open')
        back_test(s1, s2, s1_df, s2_df, 'Close')

        logging.debug('Backtested Pair #{0} {1}-{2}'.format(index, s1, s2))
    except:
        logging.error('Backtested Pair #{0} {1}-{2}'.format(index, s1, s2))




