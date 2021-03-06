from __future__ import print_function

import os
import base64
import json
import boto3
from datetime import datetime

S3_BUCKET = os.environ['S3_BUCKET']
S3_PATH = os.environ['S3_PATH']

print('Loading function')


def lambda_handler(event, context):

    #s3 client init
    s3 = boto3.client('s3')

    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        str_payload = base64.b64decode(record['kinesis']['data'])


        json_payload = json.loads(str_payload)
        

        date = datetime.fromtimestamp(json_payload['d']['sec'])
        
        filePath = S3_PATH + "/year=" + str(date.year) + "/month=" + str(date.month) + "/day=" + str(date.day) + "/hour=" + str(date.hour) + "/minute=" + str(date.minute) + "/" + str(json_payload['d']['sec']) + "_" + str(json_payload['_id']) + ".json"

        #print("path: " + filePath)
        
        s3.put_object(ContentType="application/json", Bucket=S3_BUCKET, Key=filePath, Body=str_payload)
        

    return 'Successfully processed {} records.'.format(len(event['Records']))