#!usr/bin/env python3
import boto3

def create_table(table_name, partition_key, sort_key, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
        
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': partition_key,
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': sort_key,
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': partition_key,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': sort_key,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'gsi2sk',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 10
            },
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'inverted-index',
                    'KeySchema': [
                        {
                            'AttributeName': sort_key, 'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': partition_key,
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': ['quantity', 'price', 'status', 'product_name', 'date']
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 10
                    }
                },
                {
                    'IndexName': 'status-date',
                    'KeySchema': [
                        {
                            'AttributeName': partition_key, 'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'gsi2sk', 'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': ['quantity', 'price', 'status', 'product_name', 'date']
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 10
                    }
                },
                {
                    'IndexName': 'inverted-status-date',
                    'KeySchema': [
                        {
                            'AttributeName': 'gsi2sk', 'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': partition_key, 'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': ['quantity', 'price', 'status', 'product_name', 'date']
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 10
                    }
                }
            ]
        )

        print(f"Creating {table_name}...")
        table.wait_until_exists()
        return table

    except Exception as err:
        print("{0} Table could not be created".format(table_name))
        print("Error message: {0}".format(err))
        
    
        
        
def delete_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    table.delete()
        
if __name__ == '__main__':
    table = create_table("users-orders_table", "pk", "sk")