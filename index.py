# built-in modules
import os
import re
import json
import codecs
from time import time

# installed modules
import elasticsearch

# project modules
from config import *


def parse_raw_data(raw_data):
    '''Parse raw data into a collection of documents

    Args:
        raw_data (basestring): string containing documents

    Returns:
        documents (dict): a <document_identifier, cleaned_document_text>
            dictionary
    '''

    documents = {}

    # split documents by looking for end of document
    # and start of new document tags
    raw_textuments = re.split(r'</DOC>\s*<DOC>', raw_data)

    for raw_doc in raw_documents:
        # find the document identifier
        doc_id = re.search(r'<DOCNO>\s([\w+-]+)\s</DOCNO>', raw_doc).group(1)

        #remove parent
        raw_doc = re.sub(r'<PARENT>\s([\w+-]+)\s</PARENT>', '', raw_doc)

        # split on text tags, grab what's between them
        # (i.e., the middle split)
        raw_text = re.split(r'</?TEXT>', raw_doc)[1]

        # replace HTML entities
        raw_text = re.sub(r'&blank;', ' ', raw_text)
        raw_text = re.sub(r'&\w+;', '', raw_text)

        # remove all HTML tags
        clean_text = re.sub(r'<.*?>', '', raw_text)

        # remove multiple spaces
        clean_compact_text = re.sub(r'(\r\n| )(\r\n| )*', r'\1', clean_text)

        # add document to collection
        documents[doc_id] = clean_compact_text

    return documents


def index_parsed_data(documents, index_name, es_host, es_port):
    '''Index documents in the collection, one by one.

    Args:
        documents (dict): a <document_identifier, cleaned_document_text>
            dictionary
        index_name (basestring): name of the index to add data to
        es_host (basestring): Elasticsaearch host
        es_port (int): Elasticsearch port
    '''

    # obtain a client fo elasticsearch
    es_client = elasticsearch.client.Elasticsearch(
        'http://{}:{}'.format(es_host, es_port), timeout=120
    )

    for doc_id, doc_content in documents.items():
        # the name 'content' for the content field and the doc_type are
        # specified in the mapping
        es_client.create(
            index=index_name, id=doc_id, doc_type='document',
            body={'content': doc_content}
        )


def bulk_index_parsed_data(
    documents, index_name, es_host, es_port, bulk_max_ops_cnt):
    '''Index documents in the collection in bulk.

    Args:
        documents (dict): a <document_identifier, cleaned_document_text>
            dictionary
        index_name (basestring): name of the index to add data to
        es_host (basestring): Elasticsaearch host
        es_port (int): Elasticsearch port
        bulk_max_ops_cnt (int): maximum number of operations for bulk action
    '''
    # obtain a client fo elasticsearch
    es_client = elasticsearch.client.Elasticsearch(
        'http://{}:{}'.format(es_host, es_port),
        timeout=120
    )

    # initialize an operations counter and an operations collector
    cnt_ops = 0
    opts = []

    for doc_id, doc_content in documents.items():
        # append appropriate operations
        opts.append({'create':
            { '_index': index_name, '_type': 'document', '_id' : doc_id}})
        opts.append({'content': doc_content})
        cnt_ops += 1

        if cnt_ops == bulk_max_ops_cnt:
            # do bulk operations
            es_client.bulk(body=opts)

            # empty list and reset operations count
            del opts[:]
            cnt_ops = 0

    es_client.bulk(body=opts)


def create_index(index_name, index_settings, es_host, es_port):
    '''Create an index in the cluster

    Args:
        index_name (basestring): name of the index to add data to
        index_settings (dictionary): settings for the index that
            follow the Elasticsearch specs
        es_host (basestring): Elasticsaearch host
        es_port (int): Elasticsearch port
    '''

    # obtain a client fo elasticsearch
    es_client = elasticsearch.client.Elasticsearch(
        'http://{}:{}'.format(es_host, es_port))

    # delete the previous index if it exists
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)

    # create new index
    es_client.indices.create(index=index_name, body=index_settings)


def main():
    '''Script main method'''

    # load the settings
    with open(INDEX_SETTINGS_FP) as f:
        index_settings = json.load(f)

    # get all documents
    documents = {}
    for fn in os.listdir(DATA_DIR):
        fp = os.path.join(DATA_DIR, fn)
        with open(fp) as f:
            documents.update(parse_raw_data(f.read()))

    # we time the operations
    start_time = time()

    # create the index
    create_index(INDEX_NAME, index_settings, ES_HOST, ES_PORT)

    # index data one by one
    index_parsed_data(documents, INDEX_NAME, ES_HOST, ES_PORT)

    end_time = time()
    print('one-by-one index: {:.3f} s'.format(end_time - start_time))

    # we time the operations
    start_time = time()

    # delete and recreate the index
    create_index(INDEX_NAME, index_settings, ES_HOST, ES_PORT)
    # index data in bulk
    bulk_index_parsed_data(
        documents, INDEX_NAME, ES_HOST, ES_PORT, BULK_MAX_OPS_CNT)

    end_time = time()
    print('bulk index:       {:.3f} s'.format(end_time - start_time))


if __name__ == '__main__':
    main()
