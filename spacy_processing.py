import spacy
# from mem_top import mem_top
import os
import functools
import pubmed_parser as pp
import time
from google.cloud import datastore
import sys

def read_wrapper(path):
    f = open(path, "r")
    content = f.read()
    f.close()
    return content

def extract_abstract(path):
    return pp.parse_pubmed_xml(path)['abstract']

def generate_noun_chunkset_entity(noun_chunkset, article_details, client):
    entity = datastore.Entity(client.key('noun_chunks'))
    entity['chunks'] = list(map(lambda x: x.text_with_ws, noun_chunkset))
    entity['pmid'] = article_details['pmid']
    entity['pmc'] = article_details['pmc']
    entity['doi'] = article_details['doi']
    entity['full_title'] = article_details['full_title']
    return entity

def path_proc(filelist):
    paths = map(lambda x: os.path.join(filelist[0], x), filter(lambda x: x.endswith('.nxml'), filelist[-1]))
    return list(paths)


def main(root_dir):
    nlp = spacy.load('en')

    # paths = functools.reduce(
    #     lambda x,y: x + y, filter(
    #         lambda x: len(x) > 0, map(
    #             lambda entr: path_proc(entr), os.walk(root_dir)
    #         )
    #     )
    # )
    paths = filter(
        lambda x: len(x) > 0, map(
            lambda entr: path_proc(entr), os.walk(root_dir)
        )
    )
    # print(len(paths))
    # print(mem_top())
    # for i, doc in enumerate(texts):
    #     if i % 1000 == 0:
    #         print(i)

    temp_paths = functools.reduce(lambda x, y: x + y, map(lambda x: next(paths), range(10)))
    texts = map(lambda x: x, temp_paths)

    # Just do it single-threaded
    start = time.time()
    client = datastore.Client()
    for i, path in enumerate(texts):
        art_dict = pp.parse_pubmed_xml(path)
        doc = nlp(art_dict['abstract'])
        client.put(generate_noun_chunkset_entity(doc.noun_chunks, art_dict, client))
        # Dump noun_chunks as K(art_pmid):V(noun_chunk)

        if i % 100 == 0:
            print("Documents per second: {}".format(i / (time.time() - start)))
            print(art_dict.keys())
            print(list(doc.noun_chunks))
            print(doc.ents)
            print(len(doc.ents))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
