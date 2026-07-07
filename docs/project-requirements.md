# Project Requirements

## Business Problem

A sports analytics company receives IPL match scorecards after every match.

The objective is to build a serverless AWS Data Lake that:

- Stores raw cricket datasets.
- Validates uploaded files.
- Cleans and standardizes data.
- Creates business-ready analytics.
- Supports SQL querying using Amazon Athena.

## Data Layers

Bronze
- Raw datasets

Silver
- Cleaned and standardized datasets

Gold
- Aggregated reporting datasets

## AWS Services

- Amazon S3
- AWS Lambda
- AWS Step Functions
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- Amazon CloudWatch
