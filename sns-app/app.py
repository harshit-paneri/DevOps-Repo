import boto3
import requests
import logging
import json
from botocore.exceptions import NoCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

SNS_TOPIC_ARN = 'arn:aws:sns:region:account-id:topic-name'
S3_BUCKET_NAME = 'your-s3-bucket-name'
REGION = 'your-aws-region'
AWS_ACCESS_KEY = 'your-aws-access-key'
AWS_SECRET_KEY = 'your-aws-secret-key'

sns_client = boto3.client('sns', region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

s3_client = boto3.client('s3', region_name=REGION,
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY)

def fetch_message_from_api():
    try:
        response = requests.get('https://jsonplaceholder.typicode.com/posts/1')
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch message from API: {e}")
        return None

def publish_to_sns(message):
    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure='json'
        )
        logger.info(f"Message published to SNS: {response['MessageId']}")
        return response['MessageId']
    except Exception as e:
        logger.error(f"Failed to publish message to SNS: {e}")
        return None

def log_to_s3(message, message_id):
    try:
        log_entry = f"MessageID: {message_id}, Message: {json.dumps(message)}"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"logs/{message_id}.txt",
            Body=log_entry
        )
        logger.info(f"Message logged to S3: {message_id}")
    except NoCredentialsError:
        logger.error("Credentials not available for S3.")
    except Exception as e:
        logger.error(f"Failed to log message to S3: {e}")

def main():
    message = fetch_message_from_api()
    if message:
        message_id = publish_to_sns(message)
        if message_id:
            log_to_s3(message, message_id)

if __name__ == "__main__":
    main()