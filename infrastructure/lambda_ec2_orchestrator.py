import boto3
import json

def lambda_handler(event, context):
    """
    Simplified Lambda - just starts the instance, Systemd handles the rest
    """

    ec2 = boto3.client('ec2')

    instance_id = "i-09fb9f484e58e382e"
    elastic_ip = "18.159.25.178"

    try:
        pass

    except Exception as e:
        print(f"Error in Lambda function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }