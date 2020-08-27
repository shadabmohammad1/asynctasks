

import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers 
from requests_aws4auth import AWS4Auth 


from django.core.management.base import BaseCommand, CommandError
from django.conf import settings


region = 'ap-south-1'
service = 'es'


ES_CONFIG = {
    'host': 'search-boloindya-test-6ocsxdnqobqjfug2yb5c5w46ny.ap-south-1.es.amazonaws.com',
    'indices': [{
        'name': 'user-index',
        'table': 'User',
        'index-config': {
            'settings' : {
                'number_of_shards': 2,
                'number_of_replicas': 1
            },
            'mapping': {
                'properties': {
                    'name': {'type': 'text'},
                    'username': {'type': 'keyword'},
                    'is_active': {'type': 'integer'},
                    'is_popular': {'type': 'integer'},
                    'joined_date': {'type': 'date'},
                    'follower_count': {'type': 'integer'}
                }
            }      
        },
    },{
        'name': 'vb-seen-index',
        'table': 'VBSeen',
        'index-config': {
            'settings': {
                'number_of_shards': 2,
                'number_of_replicas': 1
            },
            'mapping': {
                'properties': {
                    'user_id': {'type': 'keyword'},
                    'video_id': {'type': 'keyword'},
                    'created_at': {'type': 'date'}
                }
            }
        }
    },{
        'name': 'fvb-seen-index',
        'table': 'FVBSeen',
        'index-config': {
            'settings': {
                'number_of_shards': 2,
                'number_of_replicas': 1
            },
            'mapping': {
                'properties': {
                    'video_id': {'type': 'keyword'},
                    'view_count': {'type': 'integer'},
                    'created_at': {'type': 'date'}
                }
            }
        }
    },{
        'name': 'video-playtime-index',
        'table': 'VideoPlaytime',
        'index-config': {
            'settings': {
                'number_of_shards': 2,
                'number_of_replicas': 1
            },
            'mapping': {
                'properties': {
                    'user_id': {'type': 'keyword'},
                    'video_id': {'type': 'keyword'},
                    'playtime': {'type': 'float'},
                    'timestamp': {'type': 'date'}
                }
            }
        }
    },{
        'name': 'hashtag-index',
        'table': 'Hashtag',
        'index-config': {
            'settings': {
                'number_of_shards': 2,
                'number_of_replicas': 1
            },
            'mapping': {
                'properties': {
                    'hashtag': {'type': 'keyword'},
                    'hashtag_id': {'type': 'keyword'},
                    'is_popular': {'type': 'boolean'},
                    'is_blocked': {'type': 'boolean'},
                    'total_views': {'type': 'integer'},
                    'popular_date': {'type': 'date'},
                    'hash_counter': {'type': 'integer'}
                }
            }
        }  
    }]
}


class Command(BaseCommand):
    def __init__(self, *args, **kwargs):
        ret = super().__init__(*args, **kwargs)

        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

        self.es = Elasticsearch( 
            hosts=[{'host': ES_CONFIG.get('host'), 'port': 443}], 
            http_auth=awsauth, 
            use_ssl=True, 
            verify_certs=True, 
            connection_class=RequestsHttpConnection 
        )

        return ret


    def handle(self, *args, **options):
        for index in ES_CONFIG.get('indices'):
            self.recreate_index(index)
        
        
        
    def recreate_index(self, index):
        if self.es.indices.exists(index.get('name')):
            print("===== Deleting index %s ..."%index.get('name'))
            self.es.indices.delete(index=index.get('name'), ignore=[400, 404])

        print("===== Creating Index %s ..."%index.get('name'))
        self.es.indices.create(index=index.get('name'), body=index.get('config'))
        print("===== Index %s created successfully."%index.get('name'))
        

    def bulk_insert_doc(self, doc_list, index_name):
        bulk_body = []

        for doc in doc_list:
            bulk_body.append({'index': {'_index': index_name, '_id': doc.pop('id')}})
            bulk_body.append(doc)
            
        print(self.es.bulk(body=bulk_body, headers={"Content-Type": "application/x-ndjson"}))
            

    def get_processed_doc(self, index):
        table = index.get('table')
        doc_config = index.get('doc_config')
        id_field = doc_config.get('id_field')
        term_fields = doc_config.get('term_fields')
        


        docs = []

        for item in self.dynamo.scan(TableName=table).get("Items"):
            for key,val in item.get(doc_config.get('id_field')).items():
                _id = val
                break

            assert _id, "Id field cannot be null"

            _source = {
                attr: val
                    for attr in doc_config.get('extra_fields')
                        for key, val in item.get(attr, {}).items()
            }

            _source['term'] = " ".join([val for attr in doc_config.get('term_fields')
                                                for key, val in item.get(attr, {}).items()
                                        ])


            docs.append({
                "_index": index.get('name'),
                "_type": '_doc',
                "_id": _id,
                "_source": _source
            })
        
        
        return docs

