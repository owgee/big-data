# DynamoDB Project

## Introduction

This repository contains Python scripts for interacting with Amazon DynamoDB, a fully managed NoSQL database service. The scripts demonstrate how to create a DynamoDB table, load data into it, and query the data. These examples are intended to provide a foundational understanding of working with DynamoDB in a Python environment.

## Repository Contents

- **create_RecipeTable.py**: This script creates a DynamoDB table named `RecipeTable`. It defines the table schema, including the primary key attributes and their data types.
  
- **load_data/load_data.py**: This script loads data into the `RecipeTable` DynamoDB table. It demonstrates how to batch write items to DynamoDB, ensuring efficient data loading.
  
- **load_data/query_dynamodb.py**: This script queries data from the `RecipeTable` DynamoDB table. It includes examples of various query operations, such as filtering results based on specific criteria.

- **indian-recipe-data/test.json**: This file contains test data in JSON format for querying the DynamoDB table.

- **indian-recipe-data/train.json**: This file contains training data in JSON format for loading into the DynamoDB table.


## Prerequisites

Before running the scripts, ensure you have the following:

- An AWS account with permissions to access DynamoDB.
- AWS CLI configured with your credentials.
- Python installed on your local machine.
- Boto3 library installed. You can install it using pip:
  ```bash
  pip install boto3
