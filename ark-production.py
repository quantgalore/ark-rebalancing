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
import smtplib

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
end_date = datetime.today().strftime("%Y-%m-%d")

dates = np.array(calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d"))#.values

ticker = "ARKK"

# get the change in holdings over the last 2 trading days
holdings_request = pd.json_normalize(requests.get(f"https://arkfunds.io/api/v2/etf/holdings?symbol={ticker}&date_from={dates[-2]}&date_to={dates[-1]}").json()["holdings"])
holdings_request['weight_change'] = holdings_request.groupby('ticker')['weight'].diff()
holdings_request["abs_weight_change"] = abs(holdings_request['weight_change'])
holdings_request['share_pct_change'] = round(holdings_request.groupby('ticker')['shares'].pct_change() * 100,2)
holdings_request['absolute_share_pct_change'] = round(abs(holdings_request.groupby('ticker')['shares'].pct_change() * 100), 2)
holdings_request['absolute_share_change'] = abs(holdings_request.groupby('ticker')['shares'].diff())
holdings_request['share_change'] = holdings_request.groupby('ticker')['shares'].diff()

prior_holdings = holdings_request[(holdings_request["date"] == dates[-2])]
holdings_request = holdings_request[(holdings_request["date"] == dates[-1])].dropna()    

largest_position_change = holdings_request.sort_values(by = "absolute_share_pct_change", ascending = False).head(1)

change = largest_position_change["share_pct_change"].iloc[0]

if abs(change) > 20:

    if change > 0:
        output_string = f"{ticker} increased their stake in {largest_position_change['ticker'].iloc[0]} by {largest_position_change['share_pct_change'].iloc[0]}%"
    elif change < 0:
        output_string = f"{ticker} decreased their stake in {largest_position_change['ticker'].iloc[0]} by {largest_position_change['share_pct_change'].iloc[0]}%"
else:
    output_string = f"No significant change as of: {dates[-1]}"

def send_message(message, subject):
    EMAIL = "gmail address"
    PASSWORD = "google application password -- https://support.google.com/accounts/answer/185833?hl=en"

    recipient = EMAIL
    auth = (EMAIL, PASSWORD)

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(auth[0], auth[1])

    subject = subject
    body = message
    message = f"Subject: {subject}\n\n{body}"
    server.sendmail(from_addr = auth[0], to_addrs = recipient, msg = message)
    
send_message(message = output_string, subject = "ARK Rebalance Alert")    
    