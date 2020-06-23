import pandas as pd
import numpy as np
import datetime as dt
from decimal import Decimal
import json



def get_latest_prices(inc_df, sym_list):

    df = inc_df.sort_values(["Datetime", "Symbol"], ascending = (False, True))

    df1 = df.iloc[:15,:]

    df1['Date'] = (pd.to_datetime(df1['Datetime'])).dt.date

    df2 = df1.pivot(index='Date', columns= 'Symbol', values='Close')

    df2.reset_index(level=0, inplace=True)

    current_price = {}

    for i in sym_list:
        print("Stock Symbol: ", i)
        print("Current Stock Price: ", df2[i])
        current_price[i] = df2[i].mean()

    return current_price


def calc_daily_avg_prices(historic_df, sym_list):

    hdf1 = historic_df.sort_values(["Date"], ascending = (False))

    hdf1 = hdf1.pivot(index='Date', columns= 'Symbol', values='Close')

    hdf1 = hdf1.tail(14)

    hdf1['Date'] = hdf1.index

    hdf1.reset_index(drop=True, inplace=True)
    avg_price = {}

    for i in sym_list:
        print("Stock Symbol:", i)
        print("Daily Avg. Price: ",hdf1[i].mean())
        avg_price[i] = hdf1[i].mean()
        print("------------------------------------------")

    return avg_price


sym_list = ['AAL','AAPL','ADBE','ADI','ADP','ADSK','ALGN','ALXN','AMAT']

current_price = get_latest_prices(inc_df, sym_list)
avg_price = calc_daily_avg_prices(historic_df, sym_list)

print(current_price)
print(avg_price)

def calc_change_percentage(avg_price, current_price):
    avg_price_df = pd.DataFrame.from_dict(avg_price, orient='index')
    avg_price_df = avg_price_df.rename(columns={0: "Average_Price"})
    avg_price_df['Symbol'] = avg_price_df.index
    avg_price_df.reset_index(drop=True, inplace=True)
    avg_price_df

    current_price_df = pd.DataFrame.from_dict(current_price, orient='index')
    current_price_df = current_price_df.rename(columns={0: "Current_Price"})
    current_price_df['Symbol'] = current_price_df.index
    current_price_df.reset_index(drop=True, inplace=True)
    current_price_df

    alert_df = pd.merge(avg_price_df, current_price_df, on='Symbol')
    alert_df = alert_df[['Symbol', 'Average_Price', 'Current_Price']]
    alert_df['Change_Percentage'] = round(((alert_df['Current_Price'] - alert_df['Average_Price']) / alert_df['Average_Price']) * 100, 2)

    return alert_df

alert_df = calc_change_percentage(avg_price, current_price)
print(alert_df)
# alert_df.to_csv("alert_df.csv")

import boto3
import csv

dynamodb = boto3.resource('dynamodb', 'us-east-1')

def batch_write(table_name, rows):
    print(dynamodb)
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return True

def read_csv(csv_file, list):
    print(dynamodb)
    rows = csv.DictReader(open(csv_file))

    for row in rows:
        list.append(row)



table_name = 'alert_table'


alert_df = alert_df.to_dict('records')

alert_df = json.loads(json.dumps(alert_df), parse_float=Decimal)



items = alert_df
status = batch_write(table_name, items)

if status:
    print('Data insertion completed')
else:
    print('Data insertion not done')


