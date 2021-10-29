import json, urllib, boto3, csv
from botocore.exceptions import ClientError

# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

# Connect to the DynamoDB tables
inventoryTable = dynamodb.Table('Test');

def test_dynamo():
  id = '6aa8c5866011496c9cb3d07f158a061f17d3fe2e'
  thumbnail = 's3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3fe2e/6aa8c5866011496c9cb3d07f158a061f17d3fe2e-2021-10-28-23-thumbnail.jpge'
  resized = 's3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3fe2e/6aa8c5866011496c9cb3d07f158a061f17d3fe2e-2021-10-28-23-resized.jpge' 
  uri = ['s3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3fe2e/6aa8c5866011496c9cb3d07f158a061f17d3fe2e.jpg',thumbnail,resized]

  try:
      
      inventoryTable.put_item(
          Item={
              'id': id,  'Dog_Photos': list(resized), 'dog_thumbnails': list(thumbnail)  })

  except Exception as e:
         print(e)
         print("Unable to insert data into DynamoDB table".format(e))

test_dynamo()