import spacy
# from mem_top import mem_top
import os
import functools
import pubmed_parser as pp
import time
from google.cloud import datastore
import sys
import ujson as json

from dse.auth import *
from dse.cluster import *

def nounset_to_list(noun_chunks):
    return list(map(lambda x: x.text_with_ws, noun_chunks))

def tagged_doc_to_json(doc):
    return [{'text': w.text, 'tag': w.tag_} for w in doc]

def generate_tagged_entity(tagged_doc, article_details, client):
    entity = datastore.Entity(client.key('tagged_document'), exclude_from_indexes=['doc'])
    entity['doc'] = json.dumps(tagged_doc)
    entity['pmid'] = article_details['pmid']
    entity['pmc'] = article_details['pmc']
    entity['doi'] = article_details['doi']
    entity['full_title'] = article_details['full_title']
    return entity

def generate_noun_chunkset_entity(noun_chunkset, article_details, client):
    entity = datastore.Entity(client.key('noun_chunks'))
    entity['chunks'] = noun_chunkset
    entity['pmid'] = article_details['pmid']
    entity['pmc'] = article_details['pmc']
    entity['doi'] = article_details['doi']
    entity['full_title'] = article_details['full_title']
    return entity

def path_proc(filelist):
    paths = map(
        lambda x: os.path.join(filelist[0], x),
        filter(lambda x: x.endswith('.nxml'), filelist[-1]))
    return list(paths)

def deposit_keywords(session, keywords):
    for kw_dict in keywords:
        result = session.execute_graph('g.V().has("keyword", "content", \
            _keyword).has("tag", _tag).tryNext().orElseGet({return g.addV(label,\
            "keyword", "content", _keyword, "tag", _tag).next()})', {"_keyword": kw_dict['text'],
             "_tag": kw_dict['tag']},
            execution_profile=EXEC_PROFILE_GRAPH_DEFAULT)


def main(root_dir):
    cluster_ips = ['10.128.0.2', '10.128.0.5', '10.128.0.4', '10.128.0.6']
    auth_provider = DSEPlainTextAuthProvider(
    username=os.environ['CASSANDRA_USER'],
    password=os.environ['CASSANDRA_PASSWORD'])
    graph_name = "zeitgeist"
    ep = GraphExecutionProfile(graph_options=GraphOptions(
        graph_name=graph_name))
    cluster = Cluster(cluster_ips, auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep}
    )
    session = cluster.connect()
    nlp = spacy.load('en')
    paths = functools.reduce(
        lambda x,y: x + y, filter(
            lambda x: len(x) > 0, map(
                lambda entr: path_proc(entr), os.walk(root_dir)
            )
        )
    )

    texts = map(lambda x: x, paths)
    deposit_article_keys = ["pmid", "journal", "full_title", "pmc",
        "publisher_id", "author_list", "affiliation_list",
        "kwset", "publication_year", "doi"]

    start = time.time()
    client = datastore.Client()
    kws = []
    for i, path in enumerate(texts):
        art_dict = pp.parse_pubmed_xml(path)
        doc = nlp(unicode(art_dict['abstract']))
        nounset = nounset_to_list(doc.noun_chunks)
        # art_dict['kwset'] = nounset
        all(map(lambda x: kws.append(x), tagged_doc_to_json(doc)))
        if i % 100 == 0:
            print("Documents per second: {}".format(i / (time.time() - start)))

        if i % 2000 == 0:
            pairs = map(lambda y: {"text": y[0], "tag": y[1]},
                set(map(lambda x: (x['text'], x['tag']), kws)))
            deposit_keywords(session, pairs)
            kws = []






if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
