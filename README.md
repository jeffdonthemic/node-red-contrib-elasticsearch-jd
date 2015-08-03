# node-red-contrib-elasticsearch

A set of [Node-RED](http://www.nodered.org) nodes for Elasticsearch including search, get, exists, create, update and delete.

## Install
-------

Run the following command in the root directory of your Node-RED install

```
npm install node-red-contrib-elasticsearch-jd
```

## Usage

### Create

<p>Adds a document in a specific index, making it searchable.</p>
<p>The <code>msg.payload</code> will be used for the body of the document. The index and type can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li></ul></p>
<p>If <code>msg.documentId</code> is null, then Elasticsearch will generate an ID for the document.</p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-create">Elasticsearch documentation</a> for more information.</p>

### Delete

<p>Delete a document from a specific index based on its id.</p>
<p>The id, index and type can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentId</code> - the id of the document</li><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li></ul></p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-delete">Elasticsearch documentation</a> for more information.</p>

### Exists

<p>Returns a boolean indicating whether or not a given document exists by id.</p>
<p>The boolean property <code>msg.exists</code> is added to the message and is emitted out of the node with the original payload.</p>
<p>The id, index and type can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentId</code> - the id of the document</li><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li></ul></p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-exists">Elasticsearch documentation</a> for more information.</p>

### Get

<p>Get a document from an index based on its id.</p>
<p>The id, index and type can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentId</code> - the id of the document</li><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li></ul></p>
<p>An error is thrown if the matching document is not found.</p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-get">Elasticsearch documentation</a> for more information.</p>

### Search

<p>Search with a simple query string query.</p>
<p>The index, type and query can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li><li><code>msg.query</code> - the query string to use</li></ul></p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-search">Elasticsearch documentation</a> for more information.</p>

### Update

<p>Update parts of a document.</p>
<p>The <code>msg.payload</code> will be used for the body to update the document. The id, index and type can be configured in the node, however if left blank, the following should be set in an incoming message:<ul><li><code>msg.documentId</code> - the id of the document</li><li><code>msg.documentIndex</code> - the index to use</li><li><code>msg.documentType</code> - the type to use</li></ul></p>
<p>See the <a href="https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-update">Elasticsearch documentation</a> for more information.</p>
