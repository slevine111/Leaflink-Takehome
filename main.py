import os
from typing import List, Tuple
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values


def get_data() -> List[Tuple]:
    BUCKET_NAME = 'leafliink-data-interview-exercise'
    S3_STRING = 's3'
    s3 = boto3.client(S3_STRING)
    my_bucket = boto3.resource(S3_STRING).Bucket(BUCKET_NAME)

    my_list = []
    for item in my_bucket.objects.all():
        if item.key.endswith('gz'):
            s3_response_object = s3.get_object(Bucket=BUCKET_NAME, Key=item.key)
            item_content = s3_response_object['Body'].read().decode().strip()
            if '\n' in item_content:
                item_content_split = item_content.split('\n')
                item_content_split = item_content_split[0:len(item_content_split) - 1]
                for current_item in item_content_split:
                    item_as_dict = json.loads(current_item)
                    my_list.append(tuple(item_as_dict.values()))
            else:
                item_as_dict = json.loads(item_content)
                my_list.append(tuple(item_as_dict.values()))
    return my_list


def create_redshift_table_and_insert_data(data: List[Tuple]) -> None:
    conn = psycopg2.connect(dbname=os.environ['DATABASE'],
                            user=os.environ['USER'],
                            password=os.environ['PASSWORD'],
                            host=os.environ['HOST'],
                            port=os.environ['PORT'])
    cur = conn.cursor()
    cur.execute("""CREATE TABLE leaflink_data(
      "Meta:schema" varchar NULL,
      "Meta:version" varchar NULL,
      GdprComputed bool NULL,
      GdprSource varchar NULL,
      RemoteIP varchar NULL,
      UserAgent varchar NULL,
      Ecpm int NULL,
      Datacenter bool NULL,
      BurnIn bool NULL,
      IsValidUA bool NULL,
      "User" varchar NULL,
      UserKey int NULL,
      ClickCount int NULL,
      Id varchar NOT NULL,
      CreatedOn varchar NULL,
      EventCreatedOn varchar NULL,
      ImpressionCreatedOn varchar NULL,
      AdTypeId int NULL,
      BrandId int NULL,
      CampaignId int NULL,
      Categories varchar NULL,
      ChannelId int NULL,
      CreativeId int NULL,
      CreativePassId int NULL,
      DeliveryMode int NULL,
      FirstChannelId int NULL,
      ImpressionId varchar NULL,
      DecisionId varchar NULL,
      IsNoTrack bool NULL,
      IsTrackingCookieEvents bool NULL,
      Keywords varchar NULL,
      Device varchar NULL,
      MatchingKeywords varchar NULL,
      NetworkId int NULL,
      PassId int NULL,
      PhantomCreativePassId int NULL,
      PlacementName varchar NULL,
      PhantomPassId int NULL,
      Price float NULL,
      PriorityId int NULL,
      RateType int NULL,
      Revenue float NULL,
      ServedBy varchar NULL,
      ServedByPid int NULL,
      ServedByAsg varchar NULL,
      SiteId int NULL,
      Url varchar NULL,
      ZoneId int NULL,
      PRIMARY KEY (Id))""")
    execute_values(cur,
                   'INSERT INTO leaflink_data VALUES %s',
                   data)
    conn.close()


leaflink_data = get_data()
create_redshift_table_and_insert_data(leaflink_data)
