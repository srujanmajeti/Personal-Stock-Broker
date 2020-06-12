import boto3


def create_movie_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', 'us-east-1')

    table = dynamodb.create_table(
        TableName='incremental_data',
        KeySchema=[
            {
                'AttributeName': 'Symbol',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'datewithtime',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Symbol',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'datewithtime',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


if __name__ == '__main__':
    incremental_table = create_movie_table()
    print("Table status:", incremental_table.table_status)
