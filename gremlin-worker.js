const Gremlin = require('gremlin');
const client = Gremlin.createClient();
const gremlin = Gremlin.makeTemplateTag(client);
const zerorpc = require("zerorpc");
const _ = require('underscore');

const art_omit_ks = ['author_list', 'affiliation_list'];

var server = new zerorpc.Server({
  hello: function(name, reply) {
      reply(null, "Hello, " + name);
  },
  deposit_author: async((author, reply) => {
    const resp = await(gremlin`g.V()`);
    reply(null, `Deposited ${author.standard_name}`);
  }),
  deposit_article: async((article, reply) => {
    const art_resp = await(gremlin`g.addV('article')`);
    var kvs = _.chain(article).omit(art_omit_ks).pairs().value();
    await(gremlin`g.V(art_resp.id).property(${kvs})`);
    reply(null, `Deposited ${article.title}`);
  })
});

server.bind("tcp://0.0.0.0:4242");
