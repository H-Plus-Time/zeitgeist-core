graph = TitanFactory.open('conf/titan-cassandra-es.properties')
mgmt = graph.openManagement()
mgmt.makeVertexLabel("keyword").make()
mgmt.commit()
mgmt = graph.openManagement()
mgmt.makePropertyKey('pmc').dataType(String.class).make()
mgmt.makePropertyKey('doi').dataType(String.class).make()
mgmt.makePropertyKey('pmid').dataType(String.class).make()
mgmt.makePropertyKey('full_title').dataType(String.class).make()
mgmt.makePropertyKey('publication_year').dataType(String.class).make()
mgmt.makePropertyKey('content').dataType(String.class).make()
mgmt.commit()

mgmt = graph.openManagement()
graph.tx().rollback()
pmc = mgmt.getPropertyKey('pmc')
doi = mgmt.getPropertyKey('doi')
pmid = mgmt.getPropertyKey('pmid')
full_title = mgmt.getPropertyKey('full_title')
publication_year = mgmt.getPropertyKey('publication_year')
content = mgmt.getPropertyKey('content')

keyword = mgmt.getVertexLabel('keyword')

mgmt.buildIndex('byContent', Vertex.class).addKey(content).indexOnly(keyword).buildMixedIndex('search')
mgmt.buildIndex('byContentUnique', Vertex.class).addKey(content).indexOnly(keyword).unique().buildCompositeIndex()
mgmt.buildIndex('byPubIDTripleUnique', Vertex.class).addKey(pmc).addKey(pmid).addKey(doi).unique().buildCompositeIndex()
mgmt.buildIndex('byPubIDTripleElastic', Vertex.class).addKey(pmc).addKey(pmid).addKey(doi).buildMixedIndex('search')
mgmt.commit()

mgmt.awaitGraphIndexStatus(graph, 'byPubIDTripleUnique').call()
mgmt.awaitGraphIndexStatus(graph, 'byPubIDTripleElastic').call()
mgmt.awaitGraphIndexStatus(graph, 'byContent').call()
mgmt.awaitGraphIndexStatus(graph, 'byContentUnique').call()

mgmt = graph.openManagement()
mgmt.updateIndex(mgmt.getGraphIndex("byPubIDTripleUnique"), SchemaAction.REINDEX).get()
mgmt.updateIndex(mgmt.getGraphIndex("byPubIDTripleElastic"), SchemaAction.REINDEX).get()
mgmt.updateIndex(mgmt.getGraphIndex("byContent"), SchemaAction.REINDEX).get()
mgmt.updateIndex(mgmt.getGraphIndex("byContentUnique"), SchemaAction.REINDEX).get()
mgmt.commit()