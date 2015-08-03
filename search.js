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
      var documentIndex = config.documentIndex;
      var documentType = config.documentType;
      var query = config.query;
      var maxResults = config.maxResults;
      var sort = config.sort;
      var includeFields = config.includeFields;

      // check for overriding message properties
      if (msg.hasOwnProperty("documentIndex")) {
        documentIndex = msg.documentIndex;
      }
      if (msg.hasOwnProperty("documentType")) {
        documentType = msg.documentType;
      }
      if (msg.hasOwnProperty("query")) {
        query = msg.query;
      }
      if (msg.hasOwnProperty("maxResults")) {
        maxResults = msg.maxResults;
      }
      if (msg.hasOwnProperty("sort")) {
        sort = msg.sort;
      }
      if (msg.hasOwnProperty("includeFields")) {
        includeFields = msg.includeFields;
      }

      // construct the search params
      var params = {
        size: maxResults,
        sort: sort,
        _sourceInclude: includeFields
      };
      if (documentIndex != '') params.index = documentIndex;
      if (documentType != '') params.type = documentType;
      params.body = {
        query: {
          query_string:{
            query: query
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
