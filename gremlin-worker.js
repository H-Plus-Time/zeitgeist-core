var asyncawait = require('asyncawait');
const async = asyncawait.async;
const await = asyncawait.await;
const Promise = require('bluebird');
const Gremlin = require('gremlin');
const client = Gremlin.createClient();
const gremlin = Gremlin.makeTemplateTag(client);
const zerorpc = require("zerorpc");
const _ = require('underscore');


var server = new zerorpc.Server({
  hello: function(name, reply) {
      reply(null, "Hello, " + name);
  },
  deposit_author: async((author, reply) => {
    const resp = await(gremlin`g.V()`);
    reply(null, `Deposited ${author.standard_name}`);
  }),
  deposit_article: async((article, reply) => {
    const resp = await(gremlin`g.addV(label, "article", "pmid", \
    ${article.pmid}, "full_title", ${article.full_title},"pmc", ${article.pmc},\
    "doi", ${article.doi},"publication_year", ${article.publication_year})`);
    // console.log(resp[0].properties);
    // console.log(article.kwset);
    console.log(article.full_title);
    // _.map(article.kwset, (kw) => {
    //     var kw_safe = kw.toString("utf-8")
    //     console.log(kw_safe);
    //     _.every([1,2,3], () => {
    //         var x = await(gremlin`g.addV(label, "keyword", "content", ${kw_safe})`);
    //         console.log(x);
    //     })
    // });
    reply(null, `Deposited ${article.full_title}`);
  })
});

server.bind("tcp://0.0.0.0:4242");
