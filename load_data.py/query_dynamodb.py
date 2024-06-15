import boto3
from boto3.dynamodb.conditions import Attr

# Initialize a DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Specify the table name
table_name = 'Recipes'

# Get a reference to the table
table = dynamodb.Table(table_name)

# Perform a scan operation to find all "Indian" recipe ingredients
response = table.scan(
    FilterExpression=Attr('cuisine').eq('indian'),
    ProjectionExpression='ingredients'
)


# Extract and print the list of ingredients
for item in response['Items']:
    ingredients = item['ingredients']
    print(ingredients)
