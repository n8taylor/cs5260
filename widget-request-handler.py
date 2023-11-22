import json
import boto3

sqs = boto3.client('sqs', region_name='us-east-1')

# Specify the SQS queue URL
response = sqs.get_queue_url(QueueName='cs5260-requests')
queueUrl = response['QueueUrl']

def lambda_handler(event, context):
    try:
        # Parse input data from API Gateway
        data = json.loads(event['body'])
        print(data)

        # Add data to SQS queue
        # response = sqs.send_message(
        #     QueueUrl=queueUrl,
        #     MessageBody=json.dumps(data)
        # )

        # Return success response to API Gateway
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'success', 'message': 'Request added to SQS successfully'}),
        }
    except Exception as e:
        # Return failure response to API Gateway
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': f'Error: {str(e)}'}),
        }