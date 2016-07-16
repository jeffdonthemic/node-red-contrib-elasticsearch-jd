module.exports = function(RED) {

  var elasticsearch = require('elasticsearch');

  function Create(config) {
    RED.nodes.createNode(this,config);
    this.server = RED.nodes.getNode(config.server);
    var node = this;
    
    var client = new elasticsearch.Client({
      host: this.server.host,
      maxRetries: 3,
      requestTimeout: 30000
    });
    
    var tickMs = 500,
      idleTimeout = 3000,
      maxBulkSize = config.bulkSize * 2, // action + body
      buf = [],
      bufSize = 0,
      idleMs = 0,
      processedItems = 0,
      failedItems = 0,
      isProcessing = false,
      tickTimer = setInterval(onTick, tickMs);
    
    node.on('input', function(msg) {
      idleMs = 0;
      if (isValid(msg, config)) {
        buf.push(msg.action)
        buf.push(msg.payload || {})
        bufSize+=2;
      } else {
        node.warn("Dropping invalid item: " + JSON.stringify(msg))
        failedItems+=1;
      }
    });
    
    function onTick() {
      idleMs+=tickMs;
      if (bufSize >= maxBulkSize || 
         (bufSize > 0 && idleMs > idleTimeout)) {
        processBuffer();
      }
    }
    
    function processBuffer() {
      if (isProcessing) {
        return;
      } else {
        isProcessing = true;
      }
      var bulk = buf.splice(0, maxBulkSize);
      bufSize-=bulk.length;
      updateNodeStatus();
      processBulk(bulk);
    }
    
    function processBulk(bulk) {
      var opts = {
        body: bulk
      }
      client.bulk(opts, function(err, res) {
        if (err) {
          node.warn("Bulk discarded: " + err.message);
          failedItems+=bulk.length / 2;
        } else {
          if (res.errors) {
            res.items.forEach(function(item) {
              var k = Object.keys(item)[0];
              if (item[k].error) {
                node.warn(JSON.stringify(item[k].error));
                failedItems+=1;
              } else {
                processedItems+=1;
              }
            })
          } else {
            processedItems+=bulk.length / 2;
          }
          node.send({ payload: res });
        }
        isProcessing = false;
        updateNodeStatus();
      })
    }
    
    function updateNodeStatus() {
      var text = prettyNum(bufSize) + " " 
        + prettyNum(processedItems) + " " + prettyNum(failedItems);
      node.status({
        fill: failedItems > 0 ? "yellow" : "green",
        shape: isProcessing ? "ring" : "dot",
        text: text
      });
    }
  }
  
  function allObjects(arr) {
    return arr.map(function(x) {
      return (typeof x === 'object' && !x.hasOwnProperty("length"))
    }).find(function(res) { return !res }) === undefined;
  }
  
  function isValid(msg, config) {
    if (!allObjects([msg, msg.action, msg.payload])) return false;
      
    var k = Object.keys(msg.action)[0];
    var meta = msg.action[k];
    if (!meta) return false;
    
    meta._index = meta._index || config.documentIndex;
    meta._type = meta._type || config.documentType;
    if (!meta._index || !meta._type) return false;
    
    return true;
  }
  
  function prettyNum(num, digits) {
    if (typeof digits === 'undefined') digits = 1;
    var units = ['k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'],
        decimal;

    for(var i=units.length-1; i>=0; i--) {
        decimal = Math.pow(1000, i+1);
        if(num <= -decimal || num >= decimal) {
            return +(num / decimal).toFixed(digits) + units[i];
        }
    }
    return num;
  }
  
  RED.nodes.registerType("bulk", Create);
}
