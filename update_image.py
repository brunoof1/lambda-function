'''
import json, urllib, boto3, csv

# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

# Connect to the DynamoDB tables
inventoryTable = dynamodb.Table('Guarana_Dev_Table_Global_Pet');


# This handler is run every time the Lambda function is triggered
def lambda_handler(event, context):

  # Show the incoming event in the debug log
  print("Row Event received by Lambda function: ", event)
  
  print("Dumped Event received by Lambda function: " + json.dumps(event, indent=2))

  # Get the bucket and object key from the event
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
  id = '6aa8c5866011496c9cb3d07f158a061f17d3fe2e'
  tnails(bucket,key,id)


# Photo treatment
from io import BytesIO
import boto3
from PIL import Image
from datetime import datetime
import numpy as np
import os

today = datetime.now()
timedate = today.strftime('%Y-%m-%d-%H-%M-%S')

print(timedate)

def read_image_from_s3(bucket, key, region_name='us-west-2'):

    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    response = object.get()
    file_stream = response['Body']
    im = Image.open(file_stream)

    return im

def write_image_to_s3(img_array, bucket, key, region_name='ap-southeast-1'):

    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    file_stream = BytesIO()
    im = Image.fromarray(img_array)
    im.save(file_stream, format='jpeg')
    object.put(Body=file_stream.getvalue())

def tnails(bucket,key,id):

    try:
        image = read_image_from_s3(bucket, key)
        image.thumbnail((50,50))
        image.show()

        file_name, file_extension = os.path.splitext(key)
        output_name = ''.join([file_name, '-' ,timedate, '-', 'thumbnail', file_extension])

        write_image_to_s3(np.array(image), bucket, output_name)

    except IOError:
        pass

'''

import os


key = 'arlington/6aa8c5866011496c9cb3d07f158a061f17d3fe2e/6aa8c5866011496c9cb3d07f158a061f17d3fe2e.jpg'

file_name, file_extension = os.path.splitext(key)

print(file_name)


#print(file_name.split("/")[-1])

