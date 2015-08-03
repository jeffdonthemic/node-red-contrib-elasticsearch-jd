module.exports = function(RED) {

  var elasticsearch = require('elasticsearch');

  function Exists(config) {
    RED.nodes.createNode(this,config);
    this.server = RED.nodes.getNode(config.server);
    var node = this;
    this.on('input', function(msg) {

      var client = new elasticsearch.Client({
          host: this.server.host
      });

      // check for overriding message properties
      if (msg.hasOwnProperty("documentId") && config.documentId === '') {
        config.documentId = msg.documentId;
      }
      if (msg.hasOwnProperty("documentIndex") && config.documentIndex === '') {
        config.documentIndex = msg.documentIndex;
      }
      if (msg.hasOwnProperty("documentType") && config.documentType === '') {
        config.documentType = msg.documentType;
      }

      // construct the search params
      var params = {
        index: config.documentIndex,
        type: config.documentType,
        id: config.documentId
      }

      client.exists(params).then(function (resp) {
        msg.exists = resp;
        node.send(msg);
      }, function (err) {
        node.error(err);
      });

    });
  }
  RED.nodes.registerType("exists",Exists);
}
