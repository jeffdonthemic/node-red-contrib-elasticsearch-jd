module.exports = function(RED) {

  var elasticsearch = require('elasticsearch');

  function Create(config) {
    RED.nodes.createNode(this,config);
    this.server = RED.nodes.getNode(config.server);
    var node = this;
    this.on('input', function(msg) {

      var client = new elasticsearch.Client({
          host: this.server.host
      });

      // check for overriding message properties
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
        id: msg.documentId,
        body: msg.payload
      }

      client.create(params).then(function (resp) {
        msg.payload = resp;
        node.send(msg);
      }, function (err) {
        node.error(err);
      });

    });
  }
  RED.nodes.registerType("create",Create);
}
