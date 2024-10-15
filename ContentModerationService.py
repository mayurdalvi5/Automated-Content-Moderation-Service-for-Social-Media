#!pip install boto3

import boto3
import json
import psycopg2

class TextModerator:
    def __init__(self):
        self.comprehend = boto3.client('comprehend')

    def moderate(self, text):
        response = self.comprehend.detect_sentiment(Text=text, LanguageCode='en')
        if response['Sentiment'] in ['NEGATIVE', 'MIXED']:
            return True
        return False

class ImageModerator:
    def __init__(self):
        self.rekognition = boto3.client('rekognition')

    def moderate(self, image_bytes):
        response = self.rekognition.detect_moderation_labels(
            Image={'Bytes': image_bytes}
        )
        return any(label['Name'] in ['Explicit Nudity', 'Violence'] and label['Confidence'] > 80 
                   for label in response['ModerationLabels'])

class VideoModerator:
    def __init__(self):
        self.rekognition = boto3.client('rekognition')

    def moderate(self, bucket_name, video_key):
        job_response = self.rekognition.start_content_moderation(
            Video={'S3Object': {'Bucket': bucket_name, 'Name': video_key}}
        )
        return job_response['JobId']

class S3Handler:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name

    def upload_file(self, content, key):
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=content)

class DatabaseLogger:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def log_moderation_result(self, content_id, content_type, is_flagged):
        connection = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO moderation_logs (content_id, content_type, is_flagged) VALUES (%s, %s, %s)",
            (content_id, content_type, is_flagged)
        )
        connection.commit()
        connection.close()

class ModerationService:
    def __init__(self, s3_bucket, db_config):
        self.text_moderator = TextModerator()
        self.image_moderator = ImageModerator()
        self.video_moderator = VideoModerator()
        self.s3_handler = S3Handler(s3_bucket)
        self.db_logger = DatabaseLogger(
            db_config['host'], 
            db_config['database'], 
            db_config['user'], 
            db_config['password']
        )

    def moderate_text(self, content_id, text):
        is_flagged = self.text_moderator.moderate(text)
        self.db_logger.log_moderation_result(content_id, 'text', is_flagged)
        return is_flagged

    def moderate_image(self, content_id, image_bytes):
        is_flagged = self.image_moderator.moderate(image_bytes)
        self.db_logger.log_moderation_result(content_id, 'image', is_flagged)
        return is_flagged

    def moderate_video(self, content_id, video_key):
        job_id = self.video_moderator.moderate(self.s3_handler.bucket_name, video_key)
        # You may want to implement polling or SNS for job status tracking
        self.db_logger.log_moderation_result(content_id, 'video', False)
        return job_id

    def upload_to_s3(self, content, key):
        self.s3_handler.upload_file(content, key)
        return f"s3://{self.s3_handler.bucket_name}/{key}"

# Lambda handler
def lambda_handler(event, context):
    s3_bucket = 'your-s3-bucket-name'
    db_config = {
        'host': 'your-db-host',
        'database': 'your-db-name',
        'user': 'your-db-user',
        'password': 'your-db-password'
    }

    moderation_service = ModerationService(s3_bucket, db_config)

    for record in event['Records']:
        body = json.loads(record['body'])
        content_id = body['content_id']
        content_type = body['content_type']
        content_data = body['content_data']  # In production, handle encoded data

        if content_type == 'text':
            is_flagged = moderation_service.moderate_text(content_id, content_data)
        elif content_type == 'image':
            # Assuming content_data is a binary file read from S3 or another source
            is_flagged = moderation_service.moderate_image(content_id, content_data)
        elif content_type == 'video':
            # Assuming content_data is a key to the S3 bucket object
            job_id = moderation_service.moderate_video(content_id, content_data)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Moderation processing completed.')
    }
