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
            item_data = json.loads(s3_response_object['Body'].read().decode().strip())
            my_list.append(tuple(item_data.values()))
    return my_list


def create_redshift_table_and_insert_data(data: List[Tuple]) -> None:
    conn = psycopg2.connect(dbname=os.environ['DATABASE'],
                            user=os.environ['USER'],
                            password=os.environ['PASSWORD'],
                            host=os.environ['HOST'],
                            port=os.environ['PORT'])
    cur = conn.cursor()
    cur.execute("""CREATE TABLE leaflink_data(
      "Meta:schema" varchar NOT NULL,
      "Meta:version" varchar NOT NULL,
      GdprComputed bool NOT NULL,
      GdprSource varchar NOT NULL,
      RemoteIP varchar NOT NULL,
      UserAgent varchar NULL,
      Ecpm int NOT NULL,
      Datacenter bool NOT NULL,
      BurnIn bool NOT NULL,
      IsValidUA bool NOT NULL,
      "User" varchar NOT NULL,
      UserKey int NOT NULL,
      ClickCount int NOT NULL,
      Id varchar NOT NULL,
      CreatedOn varchar NOT NULL,
      EventCreatedOn varchar nOT NULL,
      ImpressionCreatedOn varchar NOT NULL,
      AdTypeId int not NULL,
      BrandId int NOT NULL,
      CampaignId int not NULL,
      Categories varchar NOT NULL,
      ChannelId int NOT NULL,
      CreativeId int NOT NULL,
      CreativePassId int NOT NULL,
      DeliveryMode int not NULL,
      FirstChannelId int NOT NULL,
      ImpressionId varchar NOT NULL,
      DecisionId varchar NOT NULL,
      IsNoTrack bool NOT NULL,
      IsTrackingCookieEvents bool NOT NULL,
      Keywords varchar NOT NULL,
      Device varchar NOT NULL,
      MatchingKeywords varchar NOT NULL,
      NetworkId int NOT NULL,
      PassId int NOT NULL,
      PhantomCreativePassId int NOT NULL,
      PlacementName varchar NOT NULL,
      PhantomPassId int not NULL,
      Price float not NULL,
      PriorityId int NOT NULL,
      RateType int NOT NULL,
      Revenue float NOT NULL,
      ServedBy varchar NOT NULL,
      ServedByPid int NOT NULL,
      ServedByAsg varchar NOT NULL,
      SiteId int NOT NULL,
      Url varchar NOT NULL,
      ZoneId int NOT NULL,
      PRIMARY KEY (Id))""")
    execute_values(cur,
                   'INSERT INTO leaflink_data VALUES %s',
                   data)
    conn.close()


leaflink_data = get_data()
create_redshift_table_and_insert_data(leaflink_data)
