from __future__ import print_function

import os
import base64
import json
import boto3
from datetime import datetime

S3_BUCKET = os.environ['S3_BUCKET']
S3_PATH = os.environ['S3_PATH']
DEBUG = os.environ['DEBUG']

print('Loading function')


def lambda_handler(event, context):

    #s3 client init
    s3 = boto3.client('s3')

    
    i=-1
    ii=-1
    s3Puts = 0

    lastDate = None
    thisDate = None
    dataToPut = ''
    recordsCount=0
    records = []

    for record in event['Records']:
        recordsCount+=1
        json_payload = json.loads(base64.b64decode(record['kinesis']['data']))
        records.append(json_payload)

    records = sorted(records, key=lambda x: int(x['d']['sec']))

    for json_payload in records:
        i+=1
        ii+=1
        
        str_payload = json.dumps(json_payload)
        date = datetime.fromtimestamp(json_payload['d']['sec'])

            
        
        thisDate = str(date.year) + str(date.month) + str(date.day) + str(date.hour) + str(date.minute)
        
        if(i==recordsCount or (lastDate!=None and thisDate!=lastDate)):
            #if is the last iteration OR this date is not in the same minute of the last record but is not the fisrt iteration
            filePath = S3_PATH + "/" + "year=" + str(date.year) + "/month=" + str(date.month) + "/day=" + str(date.day) + "/hour=" + str(date.hour) + "/minute=" + str(date.minute) + "/" + str(json_payload['d']['sec']) + "_" + str(json_payload['_id']) + ".json"
            
            if(DEBUG):
	            print("path: " + filePath)
	            print("data: " + dataToPut)
	            print("\n")
            
            r = s3.put_object(ContentType="application/json", Bucket=S3_BUCKET, Key=filePath, Body=dataToPut)
            s3Puts+=1
            dataToPut = ''
            ii=0

        
        if(ii!=0):
            str_payload = '\n' + str_payload
            
        dataToPut = dataToPut + str_payload

        lastDate = thisDate
        
    return 'Successfully processed {} records. {} put to S3.'.format(recordsCount).s3Puts