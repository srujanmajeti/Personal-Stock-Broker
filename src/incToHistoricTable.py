import pandas as pd
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv

dynamodb = boto3.resource('dynamodb', 'us-east-1')

""" Getting all the data for a day """
table = dynamodb.Table('incremental_data')
response = table.scan()
items1 = response['Items']
while 'LastEvaluatedKey' in response:
    print(response['LastEvaluatedKey'])
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items1.extend(response['Items'])

inc_df = pd.DataFrame(items1)
print("Printing Sample Real-time Data: ")
print(inc_df.head())

# creating a symbol list
sym_list = list(inc_df['Symbol'].unique())


def get_latest_prices(inc_df, sym_list):
    """ filtering only the latest results """

    df = inc_df.sort_values(["Datetime", "Symbol"], ascending = (False, True))

    df = df.head(len(sym_list))

    new_items = df.to_dict('records')

    return new_items

def batch_write(table_name, rows):
    """ Writing to the latest records to historic table in DynamoDB """
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return True

# def read_csv(csv_file, list):
#     print(dynamodb)
#     rows = csv.DictReader(open(csv_file))
#
#     for row in rows:
#         list.append(row)



# calling functions accordingly
table_name = 'historic_data'

items = get_latest_prices(inc_df, sym_list)

status = batch_write(table_name, items)

if status:
    print('Daily run successful')
else:
    print('Daily run - Error while inserting data')

