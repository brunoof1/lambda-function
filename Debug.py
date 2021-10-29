import json, urllib, boto3, csv
from botocore.exceptions import ClientError

# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

# Connect to the DynamoDB tables
inventoryTable = dynamodb.Table('Test');

def test_dynamo():
  #id = '6aa8c5866011496c9cb3d07f158a061f17d3fe2e'
  #id = '6aa8c5866011496c9cb3d07f158a061f17d3BlZl'
  id = '6aa8c5866011496c9cb3d07f158a061f17d3Blaaa'
  uri = 's3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa.jpg'
  thumbnail = 's3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa-2021-10-27-23-thumbnail.jpge'
  resized = 's3://lab-reports-api-dev/arlington/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa/6aa8c5866011496c9cb3d07f158a061f17d3Blaaa-2021-10-27-23-resized.jpge'

  
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



  #print(Dog_Photos, Dog_Thumbnails)

  
  ####################################

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

  #print('Before Filter resized', Dy_final+S3_final)

  #print('Filter resized', photo_insert)

  #print('Dynamodb resized', Dy_final)

  #print('S3 resized', S3_final)
  
  ####################################




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

  #print('Before Filter thumbnail', Dy_final2+S3_final2)

  #print('Filter thumbnail', photo_insert2)

  #print('Dynamodbthumbnail', Dy_final2)

  #print('S3 thumbnail', S3_final2)
  

   #S3_dog_photos.append(thumbnail)

  #print(type(S3_Photos))
  #print(type(Dy_Photos))

  #print('S3', S3_Photos)
  #print('Dynamodb', Dy_Photos)

  #print(S3_final)
  #print(Dy_Photos)


  #print('Dynamodb', Dy_final)
  #print('S3', S3_Photos)

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

  response = inventoryTable.get_item(Key={'id': id})
  print('Quering Dynamo Dog_Photos', response['Item']['dog_photos'], 'Quering Dynamo Dog_Thumbnails', response['Item']['dog_thumbnails'])

test_dynamo()
