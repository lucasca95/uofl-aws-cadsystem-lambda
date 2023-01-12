import boto3, json, os, sys

def lambda_handler(event, context):
    CONSOLE_ID=os.getenv("CONSOLE_ID")
    CONSOLE_KEY=os.getenv("CONSOLE_KEY")
    CONSOLE_REGION=os.getenv("CONSOLE_REGION")
    FILENAME=event['Records'][0]['s3']['object']['key']
    
    print(FILENAME, file=sys.stderr)
    print(context, file=sys.stderr)
    
    if (True):
        try:
            s3 = boto3.client(
                service_name="s3",
                region_name=CONSOLE_REGION,
                aws_access_key_id=CONSOLE_ID,
                aws_secret_access_key=CONSOLE_KEY,
            )
            print(f'Success in connecting with S3 Client')
        except Exception as ee:
            print(f'Error: {ee}')
        
        try:
            aws_batch = boto3.client(
                service_name="batch",
                region_name=CONSOLE_REGION,
                aws_access_key_id=CONSOLE_ID,
                aws_secret_access_key=CONSOLE_KEY,
            )
            print(f'Success in connecting with AWS batch Client')
        except Exception as ee:
            print(f'Error: {ee}')
            
        try:
            ssm = boto3.client(
                service_name='ssm',
                region_name=CONSOLE_REGION,
                aws_access_key_id=CONSOLE_ID,
                aws_secret_access_key=CONSOLE_KEY,
            )
            JOB_NAME=ssm.get_parameter(Name='JOB_NAME')['Parameter']['Value']
            JOB_QUEUE=ssm.get_parameter(Name='JOB_QUEUE')['Parameter']['Value']
            JOB_DEFINITION=ssm.get_parameter(Name='JOB_DEFINITION')['Parameter']['Value']
            
        except Exception as ee:
            print(f'Error: {ee}')
        
        print("New job submission")
        xx = aws_batch.submit_job(
            jobName=JOB_NAME,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            parameters={
                'FILENAME': FILENAME
            }
        )
        
    return 0