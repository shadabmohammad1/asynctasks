import os

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers 
from requests_aws4auth import AWS4Auth 
import boto3



ES_CONFIG = {
    'host': 'search-boloindya-test-6ocsxdnqobqjfug2yb5c5w46ny.ap-south-1.es.amazonaws.com',
    'indices': [{
        'name': 'user-index',
        'table': 'User',
        'config': {
            "settings" : {
                "number_of_shards": 2,
                "number_of_replicas": 1
            },
            'mappings': {
                'properties': {
                    'term': {'type': 'text'},
                    'name': {'type': 'string'},
                    'create_date': {'format': 'dateOptionalTime', 'type': 'date'},
                    'username': {'type': 'string'},
                    'email': {'type': 'string'},
                }}
        },
        'doc_config': {
            'term_fields': ['name', 'username', 'email'],
            'id_field': 'id',
            'extra_fields': ['name', 'username']
        }
    }]
}

def get_es_client():
    awsauth = AWS4Auth(os.environ['AWS_KEY'], os.environ['AWS_SECRET'], 
                    os.environ['AWS_REGION'], 'es')

    return Elasticsearch( 
        hosts=[{'host': ES_CONFIG.get('host'), 'port': 443}], 
        http_auth=awsauth, 
        use_ssl=True, 
        verify_certs=True, 
        connection_class=RequestsHttpConnection 
    )


def get_index_config(table):
    for index in ES_CONFIG.get('indices'):
        if table == index.get('table'):
            return index


def get_processed_doc(index, item):
    table = index.get('table')
    doc_config = index.get('doc_config')
    id_field = doc_config.get('id_field')
    term_fields = doc_config.get('term_fields')


    docs = []


    for key, val in item.get(doc_config.get('id_field')).items():
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



def lambda_handler(event, context):
    es = get_es_client()
    
    for record in event.get("Records"):
        arn = record.get("eventSourceARN")
        table = arn.split("/")[1]

        index_config = get_index_config(table)

        if record.get('eventName') in ['INSERT', 'MODIFY']:
            item = record.get('dynamodb', {}).get('NewImage')
            item.update(record.get('dynamodb', {}).get('Keys'))

            docs = get_processed_doc(index_config, item)

            print("ES Response ===== ", helpers.bulk(es, docs))


    return 'Successfully processed {} records.'.format(len(event['Records']))

