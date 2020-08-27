from django.core.management.base import BaseCommand, CommandError

import psycopg2
import boto3
import datetime
import uuid
import redis
import os
import json
from time import time, sleep
import threading

from celery import group

from asynctasks.celery import app


from .data_migration_config import config
from .rebuild_es_index import Command as ESCommand

redis_client = redis.Redis(**config.get('redis_config'))
dynamo_client = boto3.client('dynamodb', **config.get('dynamo_config'))

THREADS = os.cpu_count() * 2
BATCH_SIZE = 250
DYNAMO_BATCH_SIZE = 25
ALLOWED_THREADS = 10


redis_client.set('migration_threads', 0)
lock = redis_client.lock('migration_lock')


class Command(BaseCommand):
    help = "Migrate data to dynamo"

    dynamo_type_map = {
        int: "N",
        bool: "BOOL",
        str: "S",
        float: "N"
    }

    def get_rds_cursor(self):
        rds_conn = psycopg2.connect(**config.get('rds_config'))
        rds_conn.autocommit = True

        return rds_conn.cursor()


    def check_for_migrate_column(self, cr, table_name):
        cr.execute("""
            SELECT column_name 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = %s and 
                column_name = 'is_migrated' """, [table_name])

        if not cr.fetchall():
            print(" Creating column is_migrated for table %s"%table_name)
            cr.execute("""
                ALTER TABLE %s ADD COLUMN "is_migrated" boolean default false
            """%table_name)

            cr.execute("CREATE INDEX %s_is_migrated_index ON %s (is_migrated)"%(table_name, table_name))


    def get_dynamo_row(self, value, is_key_attr=False):
        if value == '' and is_key_attr or value == None:
            return None

        value_type = None
        _type = type(value)

        if _type == datetime.datetime:
            value_type, value = 'S', str(value)

        elif _type == bytes:
            value_type, value = 'S', value.decode() 

        elif _type in (int, float):
            value_type, value = 'N', str(value)

        else:
            value_type = self.dynamo_type_map.get(_type)


        return { value_type: value }


    def get_rds_query(self, query, start_index, end_index, rds_table_name):
        return """
            SELECT %s FROM %s 
            WHERE %s AND is_migrated = false AND %s.id >= %s 
                    AND %s.id <= %s LIMIT %s 
        """%(query.get('select'), query.get('from'), query.get('where'), rds_table_name, start_index, rds_table_name, end_index, query.get('limit'))


    def test(self):
        pass


    def handle(self, *args, **options):
        _cr = self.get_rds_cursor()
        t = time()

        for table_info in config.get('table'):
            if not table_info.get('is_active'):
                continue

            lock.acquire(blocking=True)

            self.check_for_migrate_column(_cr, table_info.get('rds_table_name'))

            dynamo_table = dynamo_client.describe_table(TableName=table_info.get('dynamo_table_name'))
            dynamo_table_key_attr = {i.get('AttributeName'): i.get('AttributeType') for i in dynamo_table.get('Table').get('AttributeDefinitions')}

            query = table_info.get('rds_query')

            _cr.execute(" select count(1) from %s where %s "%(table_info.get('rds_table_name'), 
                                query.get('where')))
            count = _cr.fetchall()[0][0]

            if not count:
                continue

            _cr.execute(" select %s.id from %s where %s order by id asc limit 1"%(
                    table_info.get('rds_table_name'), table_info.get('rds_table_name'), query.get('where')))
            start_index = _cr.fetchall()[0][0]

            _cr.execute(" select %s.id from %s where %s order by id desc limit 1"%(
                    table_info.get('rds_table_name'), table_info.get('rds_table_name'), query.get('where')))
            end_index = _cr.fetchall()[0][0]

            if count < query.get('limit'):
                run_thread.delay("==", start_index, end_index, table_info, dynamo_table_key_attr)
            else:
                segments_partition = list(range(start_index, end_index, int((end_index-start_index)/THREADS))) + [end_index]
                
                res = group(run_thread.s("%s ::"%i, segments_partition[i], segments_partition[i+1],
                    table_info, dynamo_table_key_attr) for i in range(0, len(segments_partition)-1))()

            # lock.release()


        print("Total time = ", time() - t)

        if not _cr.closed:
            _cr.close()


    def add(self, a, b):
        print("a == ", a)
        print("b == ", b)


    def start_migrating_data_for_table(self, process_name, start_index, end_index, table_info, dynamo_table_key_attr):
        print(" processing table ", table_info.get('rds_table_name'))
        _cr = self.get_rds_cursor()

        t = time()

        while True:
            query = self.get_rds_query(table_info.get('rds_query'), start_index, end_index, table_info.get('rds_table_name'))

            _cr.execute(query)
            columns_list = [d.name for d in _cr.description]

            dynamo_item_list = []
            es_doc_list = []
            redis_items = {}
            migrated_ids = []
            is_data_processed = False

            for row in _cr.fetchall():
                row_data = dict(zip(columns_list, row))
                dynamo_item = self.get_formatted_item(row_data, dynamo_table_key_attr, 
                                    table_info.get('redis_get_list', []))
                dynamo_item_list.append(dynamo_item)
                item = dynamo_item.get('PutRequest').get('Item')

                for redis_save in table_info.get('redis_save_list', []):
                    prefix = redis_save.get('prefix')
                    rds_value = list(item.get(redis_save.get('key'), {}).values())[0]
                    updated_value = list(item.get(redis_save.get('assign')).values())[0]
                    redis_items[prefix + rds_value] = updated_value

                if table_info.get('elasticsearch'):
                    _doc = {}

                    for key in table_info.get('elasticsearch').get('keys'):
                        _doc[key] = row_data.get(key)

                    _doc['id'] = list(item.get('id').values())[0]

                    es_doc_list.append(_doc)

                is_data_processed = True
                migrated_ids.append(row[0])

            if not is_data_processed:
                break

            ESCommand().bulk_insert_doc(es_doc_list, table_info.get('elasticsearch').get('index'))


            # self.migrate_by_threading(dynamo_item_list, table_info)

            # if redis_items:
            #     redis_client.mset(redis_items)

            # _cr.execute("UPDATE " + table_info.get('rds_table_name') + " SET is_migrated = true WHERE id in %s", 
            #     [tuple(migrated_ids)])
            break

        print(" time = %s"%(time() - t))

        if not _cr.closed:
            _cr.close()


    def migrate_by_threading(self, dynamo_item_list, table_info):
        if len(dynamo_item_list) <= BATCH_SIZE:
            thread = threading.Thread(target=run_dynamo_batch_write_thread, 
                        args=(table_info.get('dynamo_table_name'), 
                    dynamo_item_list, table_info.get('rds_table_name')))
            thread.start()

        else:
            index_list = list(range(0, len(dynamo_item_list) + BATCH_SIZE, BATCH_SIZE))
            for i in range(len(index_list)-1):
                thread = threading.Thread(target=run_dynamo_batch_write_thread, 
                        args=(table_info.get('dynamo_table_name'), 
                            dynamo_item_list[index_list[i]: index_list[i+1]], 
                            table_info.get('rds_table_name')))

                while int(redis_client.get('migration_threads').decode()) >= ALLOWED_THREADS:
                    print(" threads =", redis_client.get('migration_threads'))
                    sleep(3)

                redis_client.set('migration_threads', int(redis_client.get('migration_threads').decode())+1)
                thread.start()


    def get_formatted_item(self, row_data, dynamo_table_key_attr, redis_get_list=[]):
        _dict = {}

        for key, val in row_data.items():

            if key == 'is_migrated':
                continue

            for redis_get in redis_get_list:
                if key == redis_get.get('key'):
                    val = redis_client.get(redis_get.get('redis_key')%(val))

            res = self.get_dynamo_row(val, dynamo_table_key_attr.get(key, False))

            if not res:
                continue

            _dict[key] = res

        _dict['id'] = {'S': str(uuid.uuid4())}

        return {'PutRequest': {'Item': _dict}}

def get_rds_cursor():
    rds_conn = psycopg2.connect(**config.get('rds_config'))
    rds_conn.autocommit = True

    return rds_conn.cursor()


@app.task
def run_thread(*args):
    Command().start_migrating_data_for_table(*args)


@app.task
def run_dynamo_batch_write_thread(dynamo_table_name, item_list, rds_table_name):
    start_index = 0
    end_index = DYNAMO_BATCH_SIZE
    item_list_length = len(item_list)

    while start_index < item_list_length:
        dynamo_request_data = {"RequestItems": {dynamo_table_name: item_list[start_index: end_index]}}

        try:
            dynamo_client.batch_write_item(**dynamo_request_data)
        except Exception as e:
            print(e)
            migrated_ids = [item.get('PutRequest').get('Item').get('rds_id').get('N')  for item in item_list[start_index: end_index]]

            print(" migrated_ids ", migrated_ids)
            cr = get_rds_cursor()
            cr.execute("UPDATE " + rds_table_name + " SET is_migrated = false WHERE id in %s and is_migrated = true ", [tuple(migrated_ids)])

        start_index, end_index = end_index, end_index + DYNAMO_BATCH_SIZE

    print("%s Item updated"%item_list_length)

    running_threads = int(redis_client.get('migration_threads').decode())

    if running_threads <= 1:
        lock.release()
    else:
        redis_client.set('migration_threads', running_threads - 1)
