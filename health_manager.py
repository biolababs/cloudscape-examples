import json
import boto3
from datetime import datetime, timezone

# Initialize the boto3 clients
health_client = boto3.client('health')
dynamodb = boto3.resource('dynamodb')

# DynamoDB table name
table_name = 'InfrastructureStack-ItemsTable5AAC2C46-7XXF2NMJU66J'
table = dynamodb.Table(table_name)

def describe_event_details(health_client, event_arn):
    response = health_client.describe_event_details(eventArns=[event_arn])
    if response['successfulSet']:
        event_details = response['successfulSet'][0]
        event_description = event_details['eventDescription']['latestDescription']
        return event_description
    else:
        return ''

def lambda_handler(event, context):
    # Get all events from AWS Health API
    response = health_client.describe_events()

    for event_detail in response['events']:
        # Extract relevant details from the event
        event_arn = event_detail['arn']
        event_service = event_detail['service']
        event_type_code = event_detail['eventTypeCode']
        event_type_category = event_detail['eventTypeCategory']
        event_status_code = event_detail['statusCode']
        event_scope_code = event_detail['eventScopeCode']

        # Get event description from describe_event_details API
        event_description = describe_event_details(health_client, event_arn)

        # Check if the event already exists in the DynamoDB table
        existing_item = table.get_item(Key={'itemId': event_arn})

        if 'Item' in existing_item:
            # Event already exists, check if the status has changed
            existing_status = existing_item['Item']['status']
            if existing_status != event_status_code.lower():
                # Update the status, updated_at timestamp, and event description
                item = {
                    'itemId': event_arn,
                    'created_at': existing_item['Item']['created_at'],
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                    'name': event_service,
                    'type': event_type_category.capitalize(),
                    'status': event_status_code.lower(),
                    'details': event_description
                }
                table.put_item(Item=item)
        else:
            # Event doesn't exist, create a new item
            item = {
                'itemId': event_arn,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'name': event_service,
                'type': event_type_category.capitalize(),
                'status': event_status_code.lower(),
                'details': event_description
            }
            table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps('AWS Health events written to DynamoDB')
    }
