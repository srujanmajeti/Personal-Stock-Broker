
from decimal import Decimal
import json
import pandas as pd
import yfinance as yf
import json
import datetime as dt
from datetime import datetime



def get_data_from_api():
    hist_final = pd.DataFrame()
    
    sym_list = ['A', 'AAL', 'AAP']

    

    count = 0
    for i in sym_list:
        count += 1
        print("count: ", count)
        j = yf.Ticker(i)
        hist = j.history(period="1mo")
        hist['Symbol'] = i
        print(hist.head())
        # hist_temp =  hist.tail(1)
        # print(hist_temp)
        temp = [hist_final, hist]
        hist_final = pd.concat(temp)

    return hist_final


def preprocessing(hist_final):


    # converting dataframe to list of dictionaries
    list_of_dicts = hist_final.to_dict(orient='records')

    # adding date-index as a column to the dataframe
    hist_final['Date'] = hist_final.index
    
    # resetting the indexes
    hist_final.reset_index(drop=True, inplace=True)
    
    # print(hist_final)

    # changing float to decimal to push data into dynamodb
    ddb_data = json.loads(json.dumps(list_of_dicts), parse_float=Decimal)

    # adding the 'date' column after converting float to decimals
    ddb_data_df = pd.DataFrame(ddb_data)
    ddb_data_df['Date'] = hist_final['Date']
    
    ddb_data_df['Date'] = ddb_data_df['Date'].astype(str)
    dicts = ddb_data_df.to_dict(orient='records')
    print("dicts:")
    print(dicts)

    return dicts


import boto3
import csv

dynamodb = boto3.resource('dynamodb', 'us-east-1')

def batch_write(table_name, rows):

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


if __name__ == '__main__':

    table_name = 'historic_data'
    # file_name = 'hist_final_1mo.csv'

    ### calculating time taken to execute
    begin_time1 = dt.datetime.now()
    # start_date = datetime.date(datetime.now())

    hist_final = get_data_from_api()

    ### calculating time taken to execute
    print("Time taken to execute api calls: ", dt.datetime.now() - begin_time1)
    begin_time2 = dt.datetime.now()

    items = preprocessing(hist_final)

    # read_csv(file_name, items)
    status = batch_write(table_name, items)

    if status:
        print('Data insertion successful')
    else:
        print('Error while inserting data')

    ### calculating time taken to execute
    print("Time taken to push data to DynamoDB: ", dt.datetime.now() - begin_time2)
