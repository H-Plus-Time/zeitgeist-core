var asyncawait = require('asyncawait');
const async = asyncawait.async;
const await = asyncawait.await;
const Gremlin = require('gremlin');
const client = Gremlin.createClient();
const gremlin = Gremlin.makeTemplateTag(client);
const _ = require('underscore');
const express = require('express');
const cors = require('cors');
var app = express();
app.use(cors());
var port = process.env.PORT || 8080;


var router = express.Router();

router.get('/', function(req, res) {
    res.json({ message: 'hooray! welcome to our api!' });
});

router.get('/test', async((req,res) => {
  var x = await(gremlin`g.V()`)
  console.log(x);
  res.json({message: "Hola!"})
}))

router.get('/authors', async((req,res) => {
  var start = req.query.start || 0;
  console.log(start);
  var limit = req.query.limit || 1000;
  var authors = await(gremlin`g.V().hasLabel('author').range(${start}, ${limit})`)
  console.log(authors);
}))

router.get('/articles', async((req,res) => {
  var start = req.query.start || 0;
  var limit = req.query.limit || 1000;

  var articles = await(gremlin`g.V().hasLabel('article').range(${start}, ${limit})`)
  console.log(articles);
  res.json(articles);
}))

router.get('/kwoccurs', async((req,res) => {
  var subgraph = await(gremlin`g.E().hasLabel('occurs').subgraph('kwoccurs').cap('kwoccurs').next()`);
  res.json(subgraph);
}))


app.use('/api', router);

app.listen(port);
console.log('starting ' + port);
