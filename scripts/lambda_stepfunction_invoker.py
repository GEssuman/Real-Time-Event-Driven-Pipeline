import boto3
import time
import os

stepfunctions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    current_timestamp = int(time.time())

    response = stepfunctions.start_execution(
        stateMachineArn="arn:aws:states:eu-north-1:309797288544:stateMachine:MyStateMachine-0c176357",
        input=f'{{"event_time": {current_timestamp}}}'
    )

    return {
        "statusCode": 200,
        "body": f"Step Function started with event_time: {current_timestamp}",
        "executionArn": response["executionArn"]
    }
