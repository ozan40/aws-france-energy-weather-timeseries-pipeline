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

        # 3. Verify Elastic IP
        instances = ec2.describe_instances(InstanceIds = [instance_id])
        instance = instances['Reservations'][0]['Instances'][0]
        current_public_ip = instance.get('PublicIpAddress', 'N/A')

        print(f"Instance Public IP: {current_public_ip}")
        print(f"Expected Elastic IP: {elastic_ip}")

        # 4. Return immediately - Systemd will handle shutdown
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EC2 instance started successfully - Systemd service will handle scraping and shutdown',
                'instance_id': instance_id,
                'elastic_ip': elastic_ip,
                'note': 'Instance will automatically shutdown after scrapers complete'
            })
        }

    except Exception as e:
        print(f"Error in Lambda function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }