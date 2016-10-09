import spacy
# from mem_top import mem_top
import os
import functools
import csv
import pubmed_parser as pp
import time
from google.cloud import datastore
import sys
import ujson as json
from ctypes import c_char_p, cdll
import zerorpc


gremlin = cdll.LoadLibrary('./pylib/libgremlin.so')

def read_wrapper(path):
    f = open(path, "r")
    content = f.read()
    f.close()
    return content

def extract_abstract(path):
    return pp.parse_pubmed_xml(path)['abstract']

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


def main(root_dir):
    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:4242")
    # gremlin.connect()
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

    # Just do it single-threaded
    start = time.time()
    client = datastore.Client()
    kws = []
    num_skipped = 0
    for i, path in enumerate(texts):
        art_dict = pp.parse_pubmed_xml(path)
        doc = nlp(art_dict['abstract'])
        nounset = nounset_to_list(doc.noun_chunks)
        # art_dict['kwset'] = nounset
        tagged_json = tagged_doc_to_json(doc)
        try:
            client.put(
                generate_tagged_entity(tagged_json, art_dict, client))
        except Exception as e:
            num_skipped += 1
        # c.deposit_article(art_dict)
        # kws.append(tagged_json)

        # deposit article -> ((pmc, pmid, doi), vert_id)
        # dump Keywords
        # deposit keyword -> (keyword, vert_id)
        # sort *.out | uniq >> comb.out
        # py read arts, read Keywords
        # for art in arts:
        #   for keyword in art.keywords:
        #       addE('occurs_in', keyword.id, art.id)
        # gremlin.deposit_article(c_char_p(bytes(json.dumps(
            # { k: art_dict[k] for k in deposit_article_keys }), "utf-8")))

        # Dump noun_chunks as K(art_pmid):V(noun_chunk)


        if i % 100 == 0:
            print("Documents per second: {}".format(i / (time.time() - start)))
            # with open("keywords/{}.json".format(time.time()), "w") as f:
                # for item in kws:
                    # for kw in item:
                        # f.write('"{}", "{}"\n'.format(kw['text'], kw['tag']))

            # print(art_dict.keys())
            # print(list(doc.noun_chunks))
            # print(doc.ents)
            # print(len(doc.ents))
    print("Number skipped: {}".format(num_skipped))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
