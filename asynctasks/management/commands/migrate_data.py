from django.core.management.base import BaseCommand, CommandError

import psycopg2
import boto3
import datetime
import uuid

limit = 2

config = {
    'rds_config': {
        'database': 'boloindya',
        'user': 'boloindya',
        'password': 'bng321',
        'host': 'localhost',
        'port': '5433',  
    },
    'dynamo_config': {
        'aws_access_key_id': 'AKIAZNK4CM5CSAAO3R5C',
        'aws_secret_access_key': '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',
        'region_name': 'ap-south-1',
        # 'endpoint_url': 'http://dynamodev.boloindya.com:8000'
    },
    'table': [{
        'rds_query': 'select u.password, u.last_login, case when u.is_superuser then 1 else 0 end as is_superuser, u.username, u.first_name, u.last_name, u.email, case when u.is_staff then 1 else 0 end as is_staff, case when u.is_active then 1 else 0 end as is_active, u.date_joined, p.id as profile_id, p.slug, p.location, p.last_seen, p.last_ip, p.timezone, case when p.is_administrator then 1 else 0 end as is_administrator, case when p.is_moderator then 1 else 0 end as is_moderator, case when p.is_verified then 1 else 0 end as is_verified, p.topic_count, p.comment_count, p.user_id, p.last_post_hash, p.last_post_on, p.about, p.bio, p.extra_data, p.language, p.name, p.profile_pic, p.refrence, p.social_identifier, p.answer_count, p.bolo_score, p.follow_count, p.follower_count, p.like_count, p.question_count, p.share_count, p.mobile_no, case when p.is_geo_location then 1 else 0 end as is_geo_location, p.lang, p.lat, p.click_id, p.click_id_response, p.is_test_user, p.vb_count, case when p.is_expert then 1 else 0 end as is_expert, p.d_o_b, p.gender, p.view_count, p.linkedin_url, p.instagarm_id, p.twitter_id, p.encashable_bolo_score, case when p.is_dark_mode_enabled then 1 else 0 end as is_dark_mode_enabled, case when p.is_business then 1 else 0 end as is_business, case when p.is_popular then 1 else 0 end as is_popular, case when p.is_superstar then 1 else 0 end as is_superstar, p.cover_pic, p.total_time_spent, p.total_vb_playtime, p.own_vb_view_count, p.city_name, p.state_name, p.paytm_number, p.android_did, case when p.is_guest_user then 1 else 0 end as is_guest_user from auth_user u left join forum_user_userprofile p on p.user_id = u.id',
        'dynamo_table_name': 'User'
    }]

}


rds_conn = psycopg2.connect(**config.get('rds_config'))

dynamo_client = boto3.client('dynamodb', **config.get('dynamo_config'))



class Command(BaseCommand):
    help = "Migrate data to dynamo"

    dynamo_type_map = {
        int: "N",
        bool: "BOOL",
        str: "S",
    }


    def get_dynamo_row(self, value, is_key_attr=False):
        if value == '' and is_key_attr or value == None:
            return None

        _type = None

        if type(value) == datetime.datetime:
            value = str(value)
            _type = "S"

        elif type(value) == int:
            _type = "N"
            value = str(value)

        else:
            _type = self.dynamo_type_map.get(type(value))

        return { _type: value }


    def handle(self, *args, **options):
        cr = rds_conn.cursor()

        for table_info in config.get('table'):
            query = table_info.get('rds_query') + " limit %s"%limit
            cr.execute(query)
            columns_list = [d.name for d in cr.description]

            dynamo_table = dynamo_client.describe_table(TableName=table_info.get('dynamo_table_name'))
            dynamo_table_key_attr = {i.get('AttributeName'): i.get('AttributeType') for i in dynamo_table.get('Table').get('AttributeDefinitions')}


            dynamo_item_list = []

            for row in cr.fetchall():
                dynamo_item_list.append(self.get_formatted_item(columns_list, dynamo_table_key_attr, row))


            dynamo_request_data = {
                "RequestItems": {
                    table_info.get('dynamo_table_name'): dynamo_item_list
                }
            }
            print(dynamo_client.batch_write_item(**dynamo_request_data))



    def get_formatted_item(self, columns_list, dynamo_table_key_attr, row):
        _dict = {}

        for i, val in enumerate(row):
            key = columns_list[i]
            res = self.get_dynamo_row(val, dynamo_table_key_attr.get(key, False))

            if not res:
                continue

            _dict[key] = res

        _dict['id'] = {
            "S": str(uuid.uuid4())
        }

        return {
            "PutRequest": {
                "Item": _dict
            }
        }





        