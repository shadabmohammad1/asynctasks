

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers 
from requests_aws4auth import AWS4Auth 
import boto3


from django.core.management.base import BaseCommand, CommandError
from django.conf import settings


region = 'ap-south-1'
service = 'es'

awsauth = AWS4Auth("AKIAZNK4CM5C2OZ7C274", "IqIlE9itcwGtBe4tdUHiy8xbEDHuOKtrDJjBG3el", region, service)


ES_CONFIG = {
    'host': 'search-boloindya-test-6ocsxdnqobqjfug2yb5c5w46ny.ap-south-1.es.amazonaws.com',
    'indices': [{
        'name': 'user-index',
        'table': 'User',
        'config': {
            "settings" : {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "trigram": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase","shingle"]
                        },
                        "reverse": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase","reverse"]
                        }
                    },
                    "filter": {
                        "shingle": {
                            "type": "shingle",
                            "min_shingle_size": 2,
                            "max_shingle_size": 4
                        }
                    }
                }
            },
            'mappings': {
                'properties': {
                    'term': {
                            'type': 'text',
                            'fields': {
                                'trigram': {
                                'type': 'text',
                                'analyzer': 'trigram'
                            },
                            'reverse': {
                                'type': 'text',
                                'analyzer': 'reverse'
                            }
                        }
                    },
                    'name': {'type': 'text'},
                    'create_date': {'format': 'dateOptionalTime', 'type': 'date'},
                    'username': {'type': 'text'},
                    'email': {'type': 'text'},
                }}
        },
        'doc_config': {
            'term_fields': ['name', 'username', 'email'],
            'id_field': 'id',
            'extra_fields': ['name', 'username']
        }
    }]
}


class Command(BaseCommand):
    def __init__(self, *args, **kwargs):
        ret = super().__init__(*args, **kwargs)
        self.es = Elasticsearch( 
            hosts=[{'host': ES_CONFIG.get('host'), 'port': 443}], 
            http_auth=awsauth, 
            use_ssl=True, 
            verify_certs=True, 
            connection_class=RequestsHttpConnection 
        )

        self.dynamo = boto3.client('dynamodb', aws_access_key_id="AKIAZNK4CM5C2OZ7C274",
                    aws_secret_access_key="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", region_name=region)

        return ret


    def handle(self, *args, **options):
        # for index in ES_CONFIG.get('indices'):
        #     self.recreate_index(index)
        #     docs = self.get_processed_doc(index)

        #     print(helpers.bulk(self.es, docs))

        print(self.es.search(index="user-index", body={"query": {"match": {"term": "maaz" }}}))

        print(self.es.search(index="user-index", body={
            "suggest" : {
                "text" : "maaz azmi",
                "simple_phrase" : {
                    "phrase" : {
                        "field": "term.trigram",
                        "direct_generator": [ {
                            "field": "title.trigram",
                            "suggest_mode": "always"
                        } ],
                        # "size": 4,
                        # "gram_size": 4,
                        # "highlight": {
                        #     "pre_tag": "<em>",
                        #     "post_tag": "</em>"
                        # }
                    }
                }
            }
        }))

        
    def recreate_index(self, index):
        if self.es.indices.exists(index.get('name')):
            print("===== Deleting index %s ..."%index.get('name'))
            self.es.indices.delete(index=index.get('name'), ignore=[400, 404])

        print("===== Creating Index %s ..."%index.get('name'))
        self.es.indices.create(index=index.get('name'), body=index.get('config'))
        print("===== Index %s created successfully."%index.get('name'))
        
            



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

