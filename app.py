import json, urllib, boto3, csv
from botocore.exceptions import ClientError

# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

# Connect to the DynamoDB tables
inventoryTable = dynamodb.Table('Test');


# This handler is run every time the Lambda function is triggered
def lambda_handler(event, context):

  # Show the incoming event in the debug log
  #print("Row Event received by Lambda function: ", event)
  
  #print("Dumped Event received by Lambda function: " + json.dumps(event, indent=2))

  # Get the bucket and object key from the event
  #bucket = event['Records'][0]['s3']['bucket']['name']
  bucket = urllib.parse.unquote_plus(event['Records'][0]['s3']['bucket']['name'])
  key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

  file_name, file_extension = os.path.splitext(key)
  id = file_name.split("/")[-1]
  uri = 's3://' + bucket + '/' + key

  thumbnail = tnails(bucket,key)
  resized = iresize(bucket,key)

  print(bucket,key,id)

  Photos = list()

  try:
      response = inventoryTable.get_item(Key={'id': id})
      dog_photos = response['Item']['dog_photos']
      dog_thumbnails = response['Item']['dog_thumbnails']
      city = response['Item']['city']
      dog_birthday = response['Item']['dog_birthday']
      dog_color = response['Item']['dog_color']
      dog_description = response['Item']['dog_description']
      dog_name = response['Item']['dog_name']
      dog_photos = response['Item']['dog_photos']
      dog_species = response['Item']['dog_species']
      dog_weight_in_pounds = response['Item']['dog_weight_in_pounds']
      shelter = response['Item']['shelter']
      state = response['Item']['state']
      timestamp = response['Item']['timestamp']

      # Dog_Photos Parse

      Dy_dog_photos = list()
      Dy_dog_photos.append(dog_photos)

      S3_dog_photos = list()
      S3_dog_photos.append(resized)


      Dy_final = list()
      for Dy in Dy_dog_photos:
          for i in Dy:
              Dy_final.append(i)

      S3_final = list()

      for S3 in S3_dog_photos:
          if S3 not in Dy_final:
              S3_final.append(S3)

      photo_insert = list(filter(lambda k: 'jpge' in k, Dy_final+S3_final))

      # Dog_Thumbnails Parse


      Dy_dog_thumbnails = list()
      Dy_dog_thumbnails.append(dog_thumbnails)

      S3_dog_thumbnails = list()
      S3_dog_thumbnails.append(thumbnail)


      Dy_final2 = list()
      for Dy in Dy_dog_thumbnails:
          for i in Dy:
              Dy_final2.append(i)

      S3_final2 = list()

      for S3 in S3_dog_thumbnails:
          if S3 not in Dy_final2:
              S3_final2.append(S3)

      photo_insert2 = list(filter(lambda k: 'jpge' in k, Dy_final2+S3_final2))

  except ClientError as e:
        print(e.response['Error']['Message'])

  try:
      
      inventoryTable.put_item(
          Item={
              'id': id,  

              'city': city, 
              'dog_birthday': dog_birthday, 
              'dog_color': dog_color,
              'dog_description': dog_description, 
              'dog_name': dog_name,
              'dog_species': dog_species, 
              'dog_weight_in_pounds': dog_weight_in_pounds,
              'shelter': shelter,
              'state': state,
              'timestamp': timestamp,
              
              'dog_photos': photo_insert, 'dog_thumbnails': photo_insert2 })

  except Exception as e:
         print(e)
         print("Unable to insert data into DynamoDB table".format(e))

# Photo treatment
from io import BytesIO
import boto3
from PIL import Image
from datetime import datetime
import numpy as np
import os

today = datetime.now()
timedate = today.strftime('%Y-%m-%d-%H')

def read_image_from_s3(bucket, key, region_name='us-west-2'):

    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    response = object.get()
    file_stream = response['Body']
    im = Image.open(file_stream)

    return im

def write_image_to_s3(img_array, bucket, key, region_name='us-west-2'):

    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    file_stream = BytesIO()
    im = Image.fromarray(img_array)
    im.save(file_stream, format='jpeg')
    object.put(Body=file_stream.getvalue())

def tnails(bucket,key):

    try:
        image1 = read_image_from_s3(bucket, key)
        image1.thumbnail((50,50))

        file_name, file_extension = os.path.splitext(key)
        output_name_thumnail = ''.join([file_name, '-' ,timedate, '-', 'thumbnail.jpge'])

        write_image_to_s3(np.array(image1), bucket, output_name_thumnail)

    except IOError:
        pass

    return output_name_thumnail

def iresize(bucket,key):

    try:
        image2 = read_image_from_s3(bucket, key)
        image2.thumbnail((400,400))

        file_name, file_extension = os.path.splitext(key)
        output_name_resized = ''.join([file_name, '-' ,timedate, '-', 'resized.jpge'])

        id = file_name.split("/")[-1]

        write_image_to_s3(np.array(image2), bucket, output_name_resized)

    except IOError:
        pass

    return output_name_resized