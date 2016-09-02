from __future__ import unicode_literals, division, print_function

# built-in modules
import re
import os
import json
import platform
import subprocess
from tempfile import NamedTemporaryFile

# installed modules
import elasticsearch

# project modules
from config import *


def parse_raw_queries(raw_data):
    '''Parse queries

    Args:
        raw_data (basestring): string containing queries

    Returns:
        queries (list): list of dictionaries; each dictionary
            is a query with the following format:
            {
                '_id': <query_indentifier>,
                'title': <query_title>,
                'desc': <query_description>,
                'narr': <query_narrative>
            }
    '''

    queries = []

    # split documents by looking for end of query
    # and start of new query tags
    raw_queries = re.split(r'</top>\s*<top>', raw_data)

    for raw_query in raw_queries:
        # find the query identifier
        q_id = int(re.search(r'<num> Number: (\d+)', raw_query).group(1))

        # find the title of the query
        q_title = re.search(r'<title> Topic: (.+)', raw_query).group(1)

        # get the description
        q_desc = re.search(
            r'<desc> Description:\n([\w\W]+)<narr>', raw_query)\
                .group(1).replace('\n', ' ').strip()

        # get the narrative
        q_narr = re.search(
            r'<narr> Narrative:\n([\w\W]+)', raw_query)\
                .group(1).replace('\n', ' ').replace('</top>', '').strip()

        queries.append(
            {'_id': q_id, 'title': q_title, 'desc': q_desc, 'narr': q_narr})

    return queries


def make_query_dsl(str):
    '''Return query in Elasticsearch DSL format

    Args:
        str (basestring): query string

    Returns:
        query (dict): query in Elasticsearch DSL format
    '''

    query = {
        'fields': [],
        'query': {
            'match': {
                'content': {
                    'query': str,
                    'operator': 'or'
                }
            }
        }
    }
    return query


def search_queries(queries, index_name, es_host, es_port):
    '''Search results for query in queries

    Args:
        queries (list): list of dictionaries; each dictionary
            is a query with the following format:
            {
                '_id': <query_indentifier>,
                'title': <query_title>,
                'desc': <query_description>,
                'narr': <query_narrative>
            }
        index_name (basestring): name of the index to add data to
        es_host (basestring): Elasticsaearch host
        es_port (int): Elasticsearch port

    Returns:
        results (dict): dictionary of query ids and relevant documents
    '''

    es_client = elasticsearch.client.Elasticsearch(
        'http://{}:{}'.format(es_host, es_port))

    results = {}

    for query in queries:

        # get the query in the correct syntax
        query_dsl = make_query_dsl(query['title'])

        # get the results in the raw form (i.e., as returned
        # by elasticsearch)
        raw_results = es_client.search(
            index=index_name, body=query_dsl, size=1000)

        # extract the documents results and add
        # them to the results dictionary
        results[query['_id']] = raw_results['hits']['hits']

    return results


def run_treceval(results, qrels_fp, treceval_fp):
    '''Run trec_eval and return its output

        Args:
           results (dict): dictionary of query ids and relevant documents
           qrels_fp (basestring): path to qrels file
           treceval_fp (basestring): path to trec_eval binary file

        Returns:
            msg_out (basestring): output of trec_eval
    '''

    # create a temporary file
    with NamedTemporaryFile(delete=False) as tmp:
        tmp_fn = tmp.name

        # write result to temporary file using trec_eval
        # submission format
        for q_id, docs in sorted(results.items()):
            for i, doc in enumerate(docs):
                tmp.write('{q_id} 0 {d_id} {i} {score:.5f} ES_DEMO\n'.format(
                    q_id=q_id, d_id=doc['_id'], i=i, score=doc['_score']))

    # check if a version of treceval named "trec_eval"
    # exists in the binary folder; if so, it means that
    # such a version was manually compiled and it should
    # be preferred. If such file does not exists, get the
    # name of the platform this system is running on and
    # attempt to use it to select the correct, pre-compiled
    # version of treceval.
    if not os.path.exists(treceval_fp):
        platform_name = platform.system()
        treceval_fp ='{}_{}'.format(treceval_fp, platform_name)

    # command to execute treceval
    cmd = ['./{}'.format(treceval_fp), qrels_fp, tmp_fn]

    # executes treceval, catches response
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    msg_out, msg_err = proc.communicate()

    # remove temporary query file
    os.remove(tmp_fn)
    # print tmp_fn

    if msg_err:
        raise OSError(msg_err)

    # return output of trec_eval
    return msg_out.strip()


def main():
    '''Script main method'''

    # load and parse queries
    with file(QUERIES_FP) as f:
        queries = parse_raw_queries(f.read())

    # get results for query
    results = search_queries(queries, INDEX_NAME, ES_HOST, ES_PORT)

    # run trec_eval
    output_treceval = run_treceval(results, QRELS_FP, TRECEVAL_FP)

    print(output_treceval)


if __name__ == '__main__':
    main()
