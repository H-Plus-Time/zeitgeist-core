import os
import falcon
import json
from dse.auth import *
from dse.cluster import *
from google.cloud import datastore

class ArticleResource:
    def __init__(self):
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
        self.session = cluster.connect()
        self.session.default_graph_row_factory=graph.single_object_row_factory()

    def on_get(self, req, resp):
        """Handles GET requests"""
        quote = {
            'quote': 'I\'ve always been more interested in the future than in the past.',
            'author': 'Grace Hopper'
        }
        # hardcode it for the moment
        result = self.session.execute_graph('g.E().range(0, 20).inV().inE().subgraph("t").cap("t").next()', {},
            execution_profile=EXEC_PROFILE_GRAPH_DEFAULT)
        print(dir(result))
        print(map(lambda x: x.next(), result))
        #resp.body = json.dumps(list(result))



api = falcon.API()
api.add_route('/articles', ArticleResource())
