# Load-Inventory Lambda function
#
# This function is triggered by an object being created in an Amazon S3 bucket.
# The file is downloaded and each line is inserted into a DynamoDB table.

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
  localFilename = '/tmp/inventory.txt'

  # Download the file from S3 to the local filesystem
  try:
    s3.meta.client.download_file(bucket, key, localFilename)
  except Exception as e:
    print(e)
    print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    raise e

  # Read the Inventory CSV file
  with open(localFilename) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')

    # Read each row in the file
    rowCount = 0
    for row in reader:
      rowCount += 1

      # Show the row in the debug log
      print(row['shelter'], row['city'], row['state'], row['dog_name'], row['dog_species'], row['shelter_entry_date'], row['dog_description'], row['dog_birthday'], row['dog_weight_in_pounds'], row['dog_color'], row['dog_photos'], row['timestamp'])

      try:
        # Insert Dog characteristics into the Inventory table

        inventoryTable.put_item(
          Item={
              'id': row['id'], 
              'Shelter': row['shelter'], 
              'City': row['city'], 
              'State': row['state'], 
              'Dog_Name': row['dog_name'], 
              'Dog_Species': row['dog_species'], 
              'Shelter_Entry_Date': row['shelter_entry_date'], 
              'Dog_Description': row['dog_description'],	
              'Dog_Birthday': row['dog_birthday'], 
              'Dog_Weight_in_pounds': int(row['dog_weight_in_pounds']), 
              'Dog_Color': row['dog_color'],	
              'Dog_Photos': row['dog_photos'], 
              'Timestamp': row['timestamp'] })

      except Exception as e:
         print(e)
         print("Unable to insert data into DynamoDB table".format(e))

    # Finished!
    return "%d counts inserted" % rowCount
