module.exports = function(RED) {
	var MongoOplog = require('mongo-oplog');
	var mongoClient = new require('mongodb').MongoClient();
	var _ = require('lodash');

	function MongoOplogNode(config) {
		RED.nodes.createNode(this, config);
		var node = this;
		this.oplog_url = config.oplog_url;
		this.mongo_url = config.mongo_url;
		this.ns = config.ns;

		this.oplog = MongoOplog(this.oplog_url, { ns: this.ns });

		this.oplog.tail();

		var sendUpsert = function(doc) {
			var id = _.get(doc, 'o2._id');
			var collection = _.tail(_.split(this.ns, '.'));
			mongoClient.connect(this.mongo_url).then(function(db) {
				db.collection(collection).findOne({ _id: id }).then(function(o) {
					var msg = {
						topic: _.get(doc, 'op', '') === 'i' ? 'insert' : 'update',
						id: id,
						payload: o
					};
					node.send(msg);
					db.close();
				});
			});
		};

		var sendDeleted = function(doc) {
			var msg = {
				topic: 'delete',
				id: _.get(doc, 'o._id')
			};
			node.send(msg);
		};

		this.oplog.on('insert', sendUpsert.bind(this));
		this.oplog.on('update', sendUpsert.bind(this));
		this.oplog.on('delete', sendDeleted.bind(this));

		var onClose = function() {
			this.oplog.destroy();
		};
		this.on('close', onClose.bind(this));
	}

	RED.nodes.registerType('mongodb-oplog', MongoOplogNode);
};
