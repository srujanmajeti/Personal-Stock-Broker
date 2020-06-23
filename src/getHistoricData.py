import boto3
import pandas as pd
import numpy as np
from boto3.dynamodb.conditions import Key, Attr

# # boto3 is the AWS SDK library for Python.
# # The "resources" interface allows for a higher-level abstraction than the low-level client interface.
# # For more details, go to http://boto3.readthedocs.io/en/latest/guide/resources.html
# dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# table = dynamodb.Table('batch_data')
#
# # When making a Query API call, we use the KeyConditionExpression parameter to specify the hash key on which we want to query.
# # We're using the Key object from the Boto3 library to specify that we want the attribute name ("Author")
# # to equal "John Grisham" by using the ".eq()" method.
# resp = table.query(KeyConditionExpression=Key('Date'))
#
# print("The query returned the following items:")
# for item in resp['Items']:
#     print(item)



dynamodb = boto3.resource('dynamodb', 'us-east-1')

table = dynamodb.Table('historic_data')
response = table.scan()
items = response['Items']
while 'LastEvaluatedKey' in response:
    print(response['LastEvaluatedKey'])
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response['Items'])



def convertToDataframe(items):
    a = pd.DataFrame(items)
    return a

df = convertToDataframe(items)
df.to_csv("historic_data.csv")


