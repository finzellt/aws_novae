import boto3
import json
import os

def lambda_handler(event, context):
    glue = boto3.client('glue')
    s3 = boto3.client('s3')
    
    database_name = os.environ.get('GLUE_DATABASE', 'default')
    s3_bucket = 'nova-data-bucket-finzell'
    s3_key = 'Individual_Novae/glue_entries_starting_with_a.json'
    
    paginator = glue.get_paginator('get_tables')
    entries = []
    
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            if table['Name'].startswith('a'):
                entries.append(table['Name'])
    
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(entries),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'body': f'Saved {len(entries)} entries to s3://{s3_bucket}/{s3_key}'
    }