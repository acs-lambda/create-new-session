import json
import boto3
from typing import Dict, Any, Union, Optional
from botocore.exceptions import ClientError
import logging
from config import logger, AWS_REGION

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb')
sessions_table = dynamodb.Table('Sessions')

class LambdaError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[{status_code}] {message}")

class AuthorizationError(Exception):
    """Custom exception for authorization failures"""
    pass

def create_response(status_code, body):
    """Creates a standard API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }

def invoke_lambda(function_name, payload, invocation_type="RequestResponse"):
    """
    Invokes another Lambda function and returns the entire response payload.
    The caller is responsible for interpreting the response.
    """
    try:
        logger.info(f"Invoking {function_name} with type {invocation_type}...")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload),
        )
        
        response_payload = response["Payload"].read().decode("utf-8")
        if not response_payload:
            return {}
            
        return json.loads(response_payload)

    except ClientError as e:
        logger.error(f"ClientError invoking {function_name}: {e.response['Error']['Message']}")
        raise LambdaError(500, f"Failed to invoke {function_name} due to a client error.")
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON response from {function_name}")
        raise LambdaError(500, f"Invalid JSON response from {function_name}.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during invocation of {function_name}: {e}")
        raise LambdaError(500, "An unexpected error occurred during Lambda invocation.")

def parse_event(event):
    """
    Parse an event by invoking the ParseEvent Lambda function.
    """
    response = invoke_lambda('ParseEvent', event)
    if response.get('statusCode') != 200:
        raise LambdaError(response.get('statusCode', 500), "Failed to parse event.")
    
    return json.loads(response.get('body', '{}'))

def authorize(user_id, session_id):
    """
    Authorize a user by invoking the Authorize Lambda function.
    """
    response = invoke_lambda('Authorize', {'user_id': user_id, 'session_id': session_id})
    body = json.loads(response.get('body', '{}'))
    
    if response.get('statusCode') != 200 or not body.get('authorized'):
        raise LambdaError(response.get('statusCode', 401), body.get('message', 'ACS: Unauthorized')) 