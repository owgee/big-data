import boto3
import json

# Initialize a DynamoDB client
client = boto3.resource('dynamodb', region_name='us-east-1')

# Define the table name
table_name = 'Recipes'
table = client.Table(table_name)


try:
    resp = client.create_table(
        TableName=table_name,
        # Declare the Primary Key in the KeySchema argument
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
        # ProvisionedThroughput controls the amount of data that can be read or written to DynamoDB per second.
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


json_file_path = 'train.json'

# Read data from the JSON file
with open(json_file_path, 'r') as json_file:
    recipes_data = json.load(json_file)

# Insert each recipe into the Recipes Table table
for recipe in recipes_data:
    id = recipe['id']
    cuisine = recipe['cuisine']
    ingredients = recipe['ingredients']

    # Define the item to insert into the table
    item = {
        'id': id,
        'cuisine': cuisine,
        'ingredients': ingredients
    }

    # Insert the item into the table
    table.put_item(Item=item)

print('Data inserted into Recipes Table table.')
