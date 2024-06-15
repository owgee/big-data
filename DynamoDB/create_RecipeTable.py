import boto3

# Initialize a DynamoDB client
client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="Recipes",
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "id",
                "KeyType": "HASH"
            }
        ],
        # Define additional attributes used in the table
        AttributeDefinitions=[
            {
                "AttributeName": "id",
                "AttributeType": "N"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        }
    )
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)
