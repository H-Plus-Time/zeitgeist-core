import zerorpc
import os
from dse.auth import DSEPlainTextAuthProvider
from dse.cluster import Cluster

class Gremlin(object):
    """ Various convenience methods to make things cooler. """

    def __init__(self):
        cluster_ips = ['10.128.0.2', '10.128.0.5', '10.128.0.4']
        auth_provider = DSEPlainTextAuthProvider(
            username=os.environ['CASSANDRA_USER'],
            password=os.environ['CASSANDRA_PASSWORD'])
        self.cluster = Cluster(cluster_ips, auth_provider=auth_provider)
        self.session = self.cluster.connect()
        print("Connected")
    def add_man(self, sentence):
        """ End a sentence with ", man!" to make it sound cooler, and
        return the result. """
        return sentence + ", man!"

    def add_42(self, n):
        """ Add 42 to an integer argument to make it cooler, and return the
        result. """
        return n + 42

    def boat(self, sentence):
        """ Replace a sentence with "I'm on a boat!", and return that,
        because it's cooler. """
        return "I'm on a boat!"

    def deposit_article(article_json):
        result = self.session.execute_graph('g.addV(label, "article", "pmid",\
            _pmid, "pmc", _pmc, "doi", _doi, "full_title", _full_title, \
            "publication_year", _publication_year).next()', {"_pmid": article['pmid'],
                "_pmc": article['pmc'], "_doi": article['doi'],
                "_full_title": article['full_title'],
                "_publication_year": article['publication_year']},
            execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        return result.id

    def deposit_keywords(keyword_file):
        kw_id_pairs = []
        with open(keyword_file, "r") as f:
            for kw in json.load(f):
                result = self.session.execute_graph('g.addV(label, "keyword",\
                    "content", _content, "tag", _tag).next()',
                    {"_content": kw.word, "_tag": kw.tag},
                    execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
                kw_id_pairs.append((kw, result.id))

        with open(keyword_file+".out", "w") as f:
            writer = csv.writer(f)
            writer.writerows(kw_id_pairs)

    def build_occurs_in_edge(article_id, keyword_id, count):
        result = self.session.execute_graph('g.V(_article_id).addE("occurs_in"\
        ).from(g.V(_keyword_id))', {"_article_id": article_id,
        "_keyword_id": keyword_id, "_count": count},
        execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)

s = zerorpc.Server(Gremlin())
s.bind("tcp://0.0.0.0:4242")
s.run()
