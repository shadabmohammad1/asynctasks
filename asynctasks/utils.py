from .models import *
from forum.topic.utils import get_redis_fcm_token
from oauth2client.service_account import ServiceAccountCredentials
import os
from django.conf import settings

import boto3


def get_token_for_user_id(user_id):
    #it will return the list of tokens for user_id
    token_list = list(FCMDevice.objects.filter(user_id=user_id, is_uninstalled=False).values_list('reg_id', flat = True))
    token = get_redis_fcm_token(user_id)
    if token:
        token_list+=[token]
    token_list= set(token_list)
    return list(token_list)



def _get_access_token():
    """Retrieve a valid access token that can be used to authorize requests.

    :return: Access token.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    os.path.join(settings.BASE_DIR, settings.FCM_CONFIG.get("file_path")), 
                    settings.FCM_CONFIG.get("auth_url")
                )

    access_token_info = credentials.get_access_token()
    request_url = settings.PUSH_NOTIFICATION_URL

    return access_token_info.access_token, request_url


def get_dynamo_client():
    if settings.AWS_ENDPOINT_URL:
        config = {'endpoint_url': settings.AWS_ENDPOINT_URL}
    else:
        config = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_REGION_NAME
        }

    return boto3.client('dynamodb', **config)


def get_weight(key):
    for item in get_dynamo_client().scan(TableName="Weight", AttributesToGet=["feature", "weight"]):
        if item.get("feature").get("S").lower() == key.lower():
            return item.get("weight").get("S")
    return 0



def update_dynamo_entry_count(table_name, attribute, value, key, client=None):
    if not client:
        client = get_dynamo_client()

    return client.update_item(
        TableName=table_name,
        Key=key,
        ExpressionAttributeNames={"#A": attribute},
        UpdateExpression="#A = #A {} {}".format("+" if value >= 0 else "-", abs(value)),
        ReturnValues="UPDATED_NEW"
    )


    