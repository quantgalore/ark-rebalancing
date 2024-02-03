# -*- coding: utf-8 -*-
"""
Created in 2024

@author: Quant Galore
"""

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy
import mysql.connector

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

start_date = "2020-12-01"
end_date = (datetime.today() - timedelta(days=45)).strftime("%Y-%m-%d")

dates = np.array(calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d"))#.values

trades = []
times = []

ticker = "ARKK"

for date in dates[1:]:
    
    try:
        
        start_time = datetime.now()
        
        previous_day = dates[np.where(dates==date)[0][0]-1]
        next_day = dates[np.where(dates==date)[0][0]+1]
        thirty_days_prior = (pd.to_datetime(date) - timedelta(days = 45)).strftime("%Y-%m-%d")
        thirty_days_after = (pd.to_datetime(date) + timedelta(days = 45)).strftime("%Y-%m-%d")
        
        # The ETF shows the next day's data. So, if a rebalance happens at 4PM on 01/01, the data for 01/02 will be the positions after the rebalance
        
        holdings_request = pd.json_normalize(requests.get(f"https://arkfunds.io/api/v2/etf/holdings?symbol={ticker}&date_from={previous_day}&date_to={date}").json()["holdings"])
        holdings_request['weight_change'] = holdings_request.groupby('ticker')['weight'].diff()
        holdings_request["abs_weight_change"] = abs(holdings_request['weight_change'])
        holdings_request['share_pct_change'] = holdings_request.groupby('ticker')['shares'].pct_change() * 100
        holdings_request['absolute_share_pct_change'] = abs(holdings_request.groupby('ticker')['shares'].pct_change() * 100)
        holdings_request['absolute_share_change'] = abs(holdings_request.groupby('ticker')['shares'].diff())
        holdings_request['share_change'] = holdings_request.groupby('ticker')['shares'].diff()
        
        prior_holdings = holdings_request[(holdings_request["date"] == previous_day)]
        holdings_request = holdings_request[(holdings_request["date"] == date)].dropna()
        
        if len(holdings_request) < 2:
            continue
        
        largest_position_change = holdings_request.sort_values(by = "absolute_share_pct_change", ascending = False).head(1)
        
        change = largest_position_change["share_pct_change"].iloc[0]
        
        # If the change in position was less than x%
        if abs(change) < 20:
            print(f"No significant change: {date}")
            continue
        
        share_pct_change = round(largest_position_change["share_pct_change"].iloc[0],2)
        
        stock_ticker = largest_position_change["ticker"].iloc[0]
        
        underlying = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{stock_ticker}/range/1/day/{date}/{thirty_days_after}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        if len(underlying) < 1:
            continue
        underlying.index = pd.to_datetime(underlying.index, unit = "ms", utc = True).tz_convert("America/New_York")
        
        trade_data = pd.DataFrame([{"date": date,
                                    "share_pct_change": share_pct_change,
                                    "trade_day_price": underlying["o"].iloc[0],
                                    "7_day_price": underlying["c"].iloc[6],
                                    "30_day_price": underlying["c"].iloc[-1],
                                    "stock_ticker": stock_ticker, "etf_ticker": largest_position_change["fund"].iloc[0]}])
        
        trades.append(trade_data)
        
        end_time = datetime.now()
        
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
        iterations_remaining = len(dates) - np.where(dates==date)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
                
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

    except Exception as error_message:
        print(error_message)
        continue

all_trades = pd.concat(trades)

sells = all_trades[all_trades["share_pct_change"] < 0].sort_values(by="date", ascending=True)
buys = all_trades[all_trades["share_pct_change"] > 0].sort_values(by="date", ascending=True)

sells["shares"] = round(1000 / sells["trade_day_price"])
sells["gross_pnl"] = (sells["trade_day_price"] - sells["7_day_price"]) * sells["shares"]

buys["shares"] = round(1000 / buys["trade_day_price"])
buys["gross_pnl"] = (buys["7_day_price"]- buys["trade_day_price"]) * buys["shares"]

final_trades = pd.concat([sells, buys]).sort_values(by="date", ascending=True)
final_trades["date"] = pd.to_datetime(final_trades["date"])
final_trades = final_trades.set_index("date")

final_trades["capital"] = 5000 + final_trades["gross_pnl"].cumsum()


plt.figure(dpi=600)
plt.xticks(rotation=45)
plt.plot(final_trades.index, final_trades["capital"])
plt.xlabel("Date")
plt.ylabel("Capital")
plt.title("ARKK Rebalancing Strategy")
plt.show()

monthly = final_trades.resample('M').count()

plt.figure(dpi=200)
plt.xticks(rotation=45)
plt.plot(monthly.index, monthly["capital"])
plt.xlabel("Month")
plt.ylabel("# of Large Rebalances (>20%)")
plt.title("ARKK rebalancing activity over time")
plt.show()