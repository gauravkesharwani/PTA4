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


# def get_long_closed_date(opendate, spread):
#     spread1 = spread[opendate:]
#     ind = spread1.index
#     condition = spread1.Open >= spread1.MA20Day
#     CloseDateOpen = ind[condition].min()
#
#     condition = spread1.Close >= spread1.MA20Day
#     CloseDateClose = ind[condition].min()
#
#     if CloseDateOpen <= CloseDateClose:
#         return CloseDateOpen
#     else:
#         return CloseDateClose
#
#
# def get_short_closed_date(opendate, spread):
#     spread1 = spread[opendate:]
#     ind = spread1.index
#     condition = spread1.Open <= spread1.MA20Day
#     CloseDateOpen = ind[condition].min()
#
#     condition = spread1.Close <= spread1.MA20Day
#     CloseDateClose = ind[condition].min()
#
#     if CloseDateOpen <= CloseDateClose:
#         return CloseDateOpen
#     else:
#         return CloseDateClose


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


def get_positions(spread, position):
    positions = spread[spread['Position'] == position]
    for index, row in positions.iterrows():

        try:
            CloseDate = get_close_date(index, spread, position)

            S1ClosePrice = get_close_price(spread, CloseDate, position, 'S1')

            S2ClosePrice = get_close_price(spread, CloseDate, position, 'S2')

            spread.loc[index, 'CloseDate'] = CloseDate
            spread.loc[index, 'S1ClosePrice'] = S1ClosePrice
            spread.loc[index, 'S2ClosePrice'] = S2ClosePrice

            # Check
            S1Shares = round(1000 / row['S1 Open'])
            S2Shares = round(1000 / row['S2 Open'])

            S1PnL = (S1ClosePrice - row['S1 Open']) * S1Shares if position == 'Long' else (row[
                                                                                               'S1 Open'] - S1ClosePrice) * S1Shares
            S2PnL = (row['S2 Open'] - S2ClosePrice) * S2Shares if position == 'Long' else (S2ClosePrice - row[
                'S2 Open']) * S2Shares

            S1Capital = row['S1 Open'] * S1Shares
            S2Capital = row['S2 Open'] * S2Shares
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
    long = (spread.Open <= spread.LowerBand)
    short = (spread.Open >= spread.UpperBand)
    # check

    spread.loc[long, 'Position'] = 'Long'
    spread.loc[short, 'Position'] = 'Short'

    get_positions(spread, 'Long')
    get_positions(spread, 'Short')


def back_test(s1, s2):
    constDate = datetime.today().date() - timedelta(days=395)
    startDate = constDate - timedelta(30)
    endDate = datetime.today().date()

    s1_df = yf.download(s1, start=startDate, end=endDate, progress=False)
    s2_df = yf.download(s2, start=startDate, end=endDate, progress=False)

    spread = pd.DataFrame(columns=['Open', 'Close'])

    n = get_spread_const(constDate, spread, s1_df, s2_df)

    spread['S1 Open'] = s1_df['Open']
    spread['S1 Close'] = s1_df['Close']
    spread['S2 Open'] = s2_df['Open']
    spread['S2 Close'] = s2_df['Close']

    spread['Open'] = s1_df['Open'] - s2_df['Open'] * n
    spread['Close'] = s1_df['Close'] - s2_df['Close'] * n

    spread['Symbol1'] = s1
    spread['Symbol2'] = s2
    spread['Constant'] = n
    spread['StartDate'] = startDate
    spread['EndDate'] = endDate
    spread['ConstDate'] = constDate

    # check
    spread['MA20Day'] = spread['Open'].rolling(window=20).mean()
    spread['20 Day STD'] = spread['Open'].rolling(window=20).std()
    # check

    spread['UpperBand'] = spread['MA20Day'] + (spread['20 Day STD'] * 2)
    spread['LowerBand'] = spread['MA20Day'] - (spread['20 Day STD'] * 2)

    # check
    long = (spread.Open <= spread.LowerBand)
    short = (spread.Open >= spread.UpperBand)
    # check

    spread.loc[long, 'Position'] = 'Long'
    spread.loc[short, 'Position'] = 'Short'

    get_positions(spread, 'Long')
    get_positions(spread, 'Short')

    # for index, row in spread.iterrows():
    #     if row['Position'] == 'Long':
    #
    #         try:
    #             CloseDate = get_long_closed_date(index, spread)
    #
    #             S1ClosePrice = spread.loc[CloseDate, 'S1 Open'] if spread.loc[CloseDate, 'Open'] >= spread.loc[
    #                 CloseDate, 'MA20Day'] else spread.loc[
    #                 CloseDate, 'S1 Close']
    #
    #             S2ClosePrice = spread.loc[CloseDate, 'S2 Open'] if spread.loc[CloseDate, 'Open'] >= spread.loc[
    #                 CloseDate, 'MA20Day'] else spread.loc[
    #                 CloseDate, 'S2 Close']
    #
    #             spread.loc[index, 'CloseDate'] = CloseDate
    #             spread.loc[index, 'S1ClosePrice'] = S1ClosePrice
    #             spread.loc[index, 'S2ClosePrice'] = S2ClosePrice
    #
    #             # Check
    #             S1Shares = round(1000 / row['S1 Open'])
    #             S2Shares = round(1000 / row['S2 Open'])
    #             S1PnL = (S1ClosePrice - row['S1 Open']) * S1Shares
    #             S2PnL = (row['S2 Open'] - S2ClosePrice) * S2Shares
    #             S1Capital = row['S1 Open'] * S1Shares
    #             S2Capital = row['S2 Open'] * S2Shares
    #             # check
    #
    #             Capital = S1Capital + S2Capital
    #             Pnl = S1PnL + S2PnL
    #             PnlPerc = Pnl * 100 / Capital
    #             delta = CloseDate - index
    #
    #             spread.loc[index, 'S1Shares'] = S1Shares
    #             spread.loc[index, 'S2Shares'] = S2Shares
    #             spread.loc[index, 'S1Capital'] = S1Capital
    #             spread.loc[index, 'S2Capital'] = S2Capital
    #
    #             spread.loc[index, 'S1PnL'] = S1PnL
    #             spread.loc[index, 'S2PnL'] = S2PnL
    #             spread.loc[index, 'PnL'] = Pnl
    #             spread.loc[index, 'PnL%'] = PnlPerc
    #             spread.loc[index, 'Days'] = delta.days
    #
    #         except:
    #             pass
    #
    #     elif row['Position'] == 'Short':
    #
    #         try:
    #             CloseDate = get_short_closed_date(index, spread)
    #
    #             S1ClosePrice = spread.loc[CloseDate, 'S1 Open'] if spread.loc[CloseDate, 'Open'] <= spread.loc[
    #                 CloseDate, 'MA20Day'] else spread.loc[
    #                 CloseDate, 'S1 Close']
    #
    #             S2ClosePrice = spread.loc[CloseDate, 'S2 Open'] if spread.loc[CloseDate, 'Open'] <= spread.loc[
    #                 CloseDate, 'MA20Day'] else spread.loc[
    #                 CloseDate, 'S2 Close']
    #
    #             spread.loc[index, 'CloseDate'] = CloseDate
    #             spread.loc[index, 'S1ClosePrice'] = S1ClosePrice
    #             spread.loc[index, 'S2ClosePrice'] = S2ClosePrice
    #
    #             # check
    #             S1Shares = round(1000 / row['S1 Open'])
    #             S2Shares = round(1000 / row['S2 Open'])
    #             S1PnL = (row['S1 Open'] - S1ClosePrice) * S1Shares
    #             S2PnL = (S2ClosePrice - row['S2 Open']) * S2Shares
    #             S1Capital = row['S1 Open'] * S1Shares
    #             S2Capital = row['S2 Open'] * S2Shares
    #             # check
    #
    #             Capital = S1Capital + S2Capital
    #             Pnl = S1PnL + S2PnL
    #             PnlPerc = Pnl * 100 / Capital
    #             delta = CloseDate - index
    #
    #             spread.loc[index, 'S1Shares'] = S1Shares
    #             spread.loc[index, 'S2Shares'] = S2Shares
    #             spread.loc[index, 'S1Capital'] = S1Capital
    #             spread.loc[index, 'S2Capital'] = S2Capital
    #
    #             spread.loc[index, 'S1PnL'] = S1PnL
    #             spread.loc[index, 'S2PnL'] = S2PnL
    #             spread.loc[index, 'PnL'] = Pnl
    #             spread.loc[index, 'PnL%'] = PnlPerc
    #             spread.loc[index, 'Days'] = delta.days
    #
    #
    #         except:
    #             pass

    spread.dropna(inplace=True)

    spread.to_csv('Data\BTResult2.csv')

    # info.to_sql(name='BackTest4', con=con, if_exists='append')


df = pd.read_sql_query(
    "select * from CorrelatedPairs where StartDate='2019-07-15' and [ADF p-value]<=0.05 and [ADF p-value]!=0", con)
df.drop_duplicates(subset=['Symbol1', 'Symbol2'], inplace=True)

pbar = ProgressBar()

for index in pbar(df.index[:1]):
    s1 = df['Symbol1'][index]
    s2 = df['Symbol2'][index]
    back_test(s1, s2)
    logging.debug('Backtested Pair #{0} {1}-{2}'.format(index, s1, s2))
