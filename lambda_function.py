import json
import time
import random
import string
import boto3
from datetime import datetime

dynamodb = boto3.client('dynamodb')

def generate_session_id():
    """Generate a unique session identifier."""
    timestamp = int(time.time() * 1000)
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{timestamp}-{random_str}"

def lambda_handler(event, context):
    """
    AWS Lambda handler for managing user sessions.
    
    This function either creates a new session or updates the TTL of an existing session
    for a given user. If a session with the same associated_account already exists,
    it will update the TTL instead of creating a new session.

    Payload Format:
    {
        "body": string (JSON string containing):
        {
            "uid": string           # Required: User's unique identifier
        }
    }

    Return Codes:
        200: Success - Existing session TTL updated
        200: Success - New session created
        400: Bad Request - Missing required fields
        500: Internal Server Error - Server-side error occurred

    Response Format:
        Success (200/201):
        {
            "statusCode": number,
            "body": string (JSON string containing):
            {
                "sessionId": string,      # The session identifier
                "message": string,        # Success message
                "isNewSession": boolean   # True for new sessions, False for updated sessions
            }
        }

        Error (400/500):
        {
            "statusCode": number,
            "body": string (JSON string containing):
            {
                "message": string  # Error description
            }
        }

    DynamoDB Schema (Sessions table):
        session_id: string (partition key)
            - Unique identifier for the session
        created_at: string (ISO format)
            - Timestamp when the session was created
        expiration: number (Unix timestamp)
            - TTL for the session (30 days from creation/update)
        associated_account: string
            - User's unique identifier

    Example Usage:
        # Create/Update session
        payload = {
            "body": json.dumps({
                "uid": "user123",
            })
        }
        response = lambda_handler(payload, context)
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        uid = body.get('uid')

        if not uid:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required fields: uid'
                })
            }

        # Calculate TTL
        now_sec = int(time.time())
        ttl = now_sec + (30 * 24 * 3600)  # 30 days

        # Check for existing session
        try:
            response = dynamodb.scan(
                TableName='Sessions',
                FilterExpression='associated_account = :uid',
                ExpressionAttributeValues={
                    ':uid': {'S': uid}
                },
                Limit=1
            )

            if response.get('Items'):
                # Update existing session's TTL
                existing_session = response['Items'][0]
                session_id = existing_session['session_id']['S']
                
                dynamodb.update_item(
                    TableName='Sessions',
                    Key={'session_id': {'S': session_id}},
                    UpdateExpression='SET expiration = :ttl',
                    ExpressionAttributeValues={
                        ':ttl': {'N': str(ttl)}
                    }
                )

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'sessionId': session_id,
                        'message': 'Existing session TTL updated',
                        'isNewSession': False
                    })
                }

        except Exception as e:
            print(f"Error checking for existing session: {str(e)}")
            # Continue to create new session if check fails

        # Create new session if none exists
        session_id = generate_session_id()
        dynamodb.put_item(
            TableName='Sessions',
            Item={
                'session_id': {'S': session_id},
                'created_at': {'S': datetime.utcnow().isoformat()},
                'expiration': {'N': str(ttl)},
                'associated_account': {'S': uid}
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'sessionId': session_id,
                'message': 'New session created successfully',
                'isNewSession': True
            })
        }

    except Exception as e:
        print(f"Error managing session: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to manage session: {str(e)}'
            })
        }
