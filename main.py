import boto3
import pandas as pd
import json

BUCKET_NAME = 'leafliink-data-interview-exercise'
S3_STRING = 's3'

s3 = boto3.client(S3_STRING)

my_bucket = boto3.resource(S3_STRING).Bucket(BUCKET_NAME)

count = 0
my_dict = {}
my_list = []
for item in my_bucket.objects.all():
    if count == 5:
        break
    # if count == 5:
    #   break
    prefix = item.key.split('/')[0]
    if prefix not in my_dict:
        my_dict[prefix] = {'key_only': 0, 'zip': 0}

    if item.key.endswith('gz'):
        my_dict[prefix]['zip'] += 1

        s3_response_object = s3.get_object(Bucket=BUCKET_NAME, Key=item.key)
        item_data = json.loads(s3_response_object['Body'].read().decode().strip())
        my_list.append(item_data)

    else:
        my_dict[prefix]['key_only'] += 1

    count += 1

print(my_list)
py = pd.DataFrame(my_list)
py.to_csv('dsf.csv')
