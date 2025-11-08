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
        # 1. Start EC2 Instance
        print(f"Starting EC2 instance: {instance_id}")
        start_response = ec2. start_instances(InstanceIds=[instance_id])
        print((f"Start response: {start_response}"))

        # 2. Wait for instance to be running
        print("Waiting for instance to reach running state...")
        waiter_running = ec2.get_waiter('instance_running')
        waiter_running.wait(InstanceIds = [instance_id])
        print("Instance is now running")

    except Exception as e:
        print(f"Error in Lambda function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }