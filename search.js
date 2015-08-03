module.exports = function(RED) {

  var elasticsearch = require('elasticsearch');

  function Search(config) {
    RED.nodes.createNode(this,config);
    this.server = RED.nodes.getNode(config.server);
    var node = this;
    this.on('input', function(msg) {

      var client = new elasticsearch.Client({
          host: this.server.host
      });

      // check for overriding message properties
      if (msg.hasOwnProperty("documentIndex")) {
        config.documentIndex = msg.documentIndex;
      }
      if (msg.hasOwnProperty("documentType")) {
        config.documentType = msg.documentType;
      }
      if (msg.hasOwnProperty("query")) {
        config.query = msg.query;
      }

      // construct the search params
      var params = {}
      if (config.documentIndex != '') params.index = config.documentIndex;
      if (config.documentType != '') params.type = config.documentType;
      params.body = {
          query: {
              query_string:{
                 query: config.query
              }
          }
      }

      node.log(JSON.stringify(params));

      client.search(params).then(function (resp) {
        msg.payload = resp.hits;
        node.send(msg);
      }, function (err) {
        node.error(err);
      });

    });
  }
  RED.nodes.registerType("search",Search);
}
