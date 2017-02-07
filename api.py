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
        "http://titangraph-1:8182/gremlin", loop, message_serializer=GraphSONMessageSerializer)
    return remote_conn


async def acquire_traverser():
    remote_conn = await acquire_conn()
    graph = driver.AsyncGraph()
    g = graph.traversal().withRemote(remote_conn)
    return g

def flatten_vertex(vert):
    properties = dict(__builtins__.map(lambda v: (v[0], v[1][0]['value']), vert['properties'].items()))
    del vert['properties']
    return {**vert, **properties}

@app.route("/")
async def test(request):
    return json({"hello": "world"})

@app.route("/authors",  methods=['POST'])
async def retrieve_authors(request):
    params = request.json
    limit = params.get('limit', 10)
    g = await acquire_traverser()
    authors = await g.V().hasLabel('author').limit(limit).toList()
    return json(__builtins__.list(__builtins__.map(flatten_vertex, authors)))

@app.route('/coauthors', methods=['POST'])
async def retrieve_coauthors(request):
    params = request.json
    g = await acquire_traverser()
    seed_author = params.get('author_id', await g.V().hasLabel('author').id().next())
    coauthors = await g.V(seed_author).both("wrote").in_("wrote").toList()
    return json(__builtins__.list(__builtins__.map(flatten_vertex, coauthors)))

@app.route('/articles', methods=['POST'])
async def retrieve_articles(request):
    params = request.json
    g = await acquire_traverser()
    limit = params.get('limit', 10)
    articles = await g.V().hasLabel('article').limit(limit).toList()
    return json(__builtins__.list(__builtins__.map(flatten_vertex, articles)))

@app.route('/search_articles', methods=['POST'])
async def search_articles(request):
    params = request.json
    conn = await acquire_conn()
    search_term = params.get('search', '')
    if search_term == '':
        return []
    bindings = {"search_term": search_term}
    script = "g.V().hasLabel('article').where(has('full_title', textContains(search_term))).limit(5)"
    resp = await conn.submit(gremlin=script, bindings=bindings)
    articles = []
    async for msg in resp:
        articles.append(flatten_vertex(msg))
    return json(__builtins__.list(__builtins__.map(flatten_vertex, articles)))

@app.route('/search_authors', methods=['POST'])
async def search_authors(request):
    params = request.json
    conn = await acquire_conn()
    search_term = params.get('search', '')
    if search_term == '':
        return []
    bindings = {"search_term": search_term}
    script = "g.V().hasLabel('author').where(has('sur_name', textContains(search_term)).or().has('first_name', textContains(search_term))).limit(5)"
    resp = await conn.submit(gremlin=script, bindings=bindings)
    authors = []
    async for msg in resp:
        authors.append(msg)
    return json(__builtins__.list(__builtins__.map(flatten_vertex, authors)))

@app.route('/coauthorship_network', methods=['POST'])
async def retrieve_coauthor_network(request):
    params = request.json
    g = await acquire_traverser()
    min_articles = params.get('min_articles', 4)
    seed_author = params.get('author_id', await g.V().hasLabel('author').where(out('wrote').count().is_(gt(min_articles))).id().next())
    authors = await g.V(seed_author).out('wrote').in_('wrote').dedup().toList()
    wrote_edges = await g.V(seed_author).out('wrote').in_('wrote').dedup().outE('wrote').toList()
    articles = await g.V(seed_author).out('wrote').in_('wrote').dedup().out('wrote').toList()
    nodes = authors + articles
    return json({"nodes": __builtins__.list(__builtins__.map(flatten_vertex, nodes)), "edges": wrote_edges})

@app.route('/coword_network', methods=['POST'])
async def retrieve_coword_network(request):
    params = request.json
    g = await acquire_traverser()
    seed_keyword = params.get('keyword_id', await g.V().hasLabel("keyword").where(out('present_in').count().is_(gt(5))).id().next())
    articles = await g.V(seed_keyword).out('present_in').toList()
    print(seed_keyword)
    nodes = []
    present_in_edges = []
    for article in articles:
        p_in_edges = await g.V(article['id']).inE('present_in').toList()
        nodes += await g.V(article['id']).in_('present_in').toList()
        present_in_edges += p_in_edges

    return json({"nodes": __builtins__.list(__builtins__.map(flatten_vertex, nodes)), "edges": present_in_edges})

if __name__ == "__main__":
    port = int(sys.argv[1])
    app.run(host="0.0.0.0", port=port)
