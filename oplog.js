module.exports = function(RED) {
	'use strict';
	//var MongoOplog = require('mongo-oplog');
	var mongoClient = new require('mongodb').MongoClient();
	var _ = require('lodash');
	var mustache = require('mustache');
	var Timestamp = require('mongodb').Timestamp;

	function _setEnv(config) {
		var result = [];
		for (var key in config) {
			if (/{{/.test(config[key])) {
				result[key] = mustache.render(config[key], process.env);
			} else {
				result[key] = config[key];
			}
		}
		return result;
	}

	function MongodbOplogNode(n) {
		var config_env = _setEnv(n);
		RED.nodes.createNode(this, config_env);
		var node = this;
		this.oplog_url = config_env.oplog_url;
		this.mongo_url = config_env.mongo_url;
		this.ns = config_env.ns;

		var sendUpsert = function(doc) {
			var id = _.get(doc, 'o2._id');
			var collection = _.last(_.split(this.ns, '.'));
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

		var streamObserver = function(data) {
			console.log(data);
			switch (data.op) {
				case 'i':
				case 'u':
					sendUpsert.apply(this, [data]);
					break;
				case 'd':
					sendDeleted.apply(this, [data]);
					break;
			}
		};

		var lastDoc = function(doc) {
			var query = {
				ns: this.ns,
				ts: {
					$gt: doc.ts || Timestamp(0, (Date.now() / 1000) | 0)
				}
			};
			this.oplog = this.localDB
				.collection('oplog.rs')
				.find(query, {
					tailable: true,
					awaitData: true,
					oplogReplay: true,
					noCursorTimeout: true,
					numberOfRetries: Number.MAX_VALUE
				})
				.stream();

			this.oplog.on('data', streamObserver.bind(this));
		};

		var processConnection = function(db) {
			this.localDB = db;
			db
				.collection('oplog.rs')
				.find({}, { ts: 1 })
				.sort({ $natural: -1 })
				.limit(1)
				.nextObject()
				.then(lastDoc.bind(this));
		};

		var startObserve = function() {
			try {
				mongoClient.connect(this.oplog_url).then(processConnection.bind(this));
			} catch (err) {
				this.error(err, msg);
			}
		};

		var onClose = function() {
			this.oplog.close();
			this.localDB.close();
		};

		this.on('input', startObserve.bind(this));
		this.on('close', onClose.bind(this));

		setTimeout(function() {
			node.emit('input', {});
		}, 100);
	}
	RED.nodes.registerType('mongodbOplog', MongodbOplogNode);
};
