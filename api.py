import pprint
import asyncio
import sys
from itertools import chain
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from goblin import driver
from goblin.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.traversal import T
from gremlin_python import statics
statics.load_statics(globals())
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin

app = Sanic()
CORS(app)

async def acquire_conn():
    loop = asyncio.get_event_loop()
    remote_conn = await driver.Connection.open(
        "http://localhost:8182/gremlin", loop, message_serializer=GraphSONMessageSerializer)
    graph = driver.AsyncGraph()
    g = graph.traversal().withRemote(remote_conn)
    return g

@app.route("/")
async def test(request):
    # art = await g.V().next()
    # print(art)
    return json({"hello": "world"})

@app.route("/authors",  methods=['POST'])
async def retrieve_authors(request):
    params = request.json
    limit = params.get('limit', 10)
    g = await acquire_conn()
    authors = await g.V().hasLabel('author').limit(limit).toList()
    return json(authors)

@app.route('/coauthors', methods=['POST'])
async def retrieve_coauthors(request):
    params = request.json
    g = await acquire_conn()
    seed_author = params.get('author_id', await g.V().hasLabel('author').id().next())
    coauthors = await g.V(seed_author).both("wrote").in_("wrote").toList()
    return json(coauthors)

@app.route('/articles', methods=['POST'])
async def retrieve_articles(request):
    params = request.json
    g = await acquire_conn()
    limit = params.get('limit', 10)
    articles = await g.V().hasLabel('article').limit(limit).toList()
    return json(articles)

@app.route('/coauthorship_network', methods=['POST'])
async def retrieve_coauthor_network(request):
    params = request.json
    g = await acquire_conn()
    min_articles = params.get('min_articles', 1)
    seed_author = params.get('author_id', await g.V().hasLabel('author').where(out('wrote').count().is_(gt(min_articles))).id().next())
    authors = await g.V(seed_author).out('wrote').in_('wrote').toList()
    wrote_edges = []
    nodes = []
    for author in authors:
        w_edges = await g.V(author['id']).outE('wrote').toList()
        articles = await g.V(author['id']).out('wrote').toList()
        wrote_edges += w_edges
        nodes += articles
        nodes += authors
    return json({"nodes": nodes, "edges": wrote_edges})

@app.route('/coword_network', methods=['POST'])
async def retrieve_coword_network(request):
    params = request.json
    g = await acquire_conn()
    seed_keyword = params.get('keyword_id', await g.V().hasLabel("keyword").where(out('present_in').count().is_(gt(5))).id().next())
    articles = await g.V(seed_keyword).out('present_in').toList()
    print(seed_keyword)
    nodes = []
    present_in_edges = []
    for article in articles:
        p_in_edges = await g.V(article['id']).inE('present_in').toList()
        nodes += await g.V(article['id']).in_('present_in').toList()
        present_in_edges += p_in_edges

    return json({"nodes": nodes, "edges": present_in_edges})
    
if __name__ == "__main__":
    port = int(sys.argv[1])
    app.run(host="0.0.0.0", port=port)
