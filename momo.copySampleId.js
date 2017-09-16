
const fs = require('fs');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const MongoClient = require('mongodb').MongoClient;
const mongoUrl = 'mongodb://localhost:27017/momo';
const FROM_COLLECTION = 'sample';
const TO_COLLECTION = 'sampleid';
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.copySampleId.log`);

class Copy extends EventEmitter {
	constructor() {
		super();
		this.logdir = './log/';
		this.db = null;
		this.stream = null;
		this.operations = [];
		this.on('MONGO_READY', this.onMongoReady.bind(this));
		this.on('MONGO_UPDATE', this.onMongoUpdate.bind(this));
	}
	onMongoUpdate(end) {
		let self = this;
		if(!self.operations.length) {
			if(end) {
				self.db.close();
				logger.info('Job done');
			} else {
				self.stream.resume();
			}
		} else {
			let bulkOps = self.db.collection(TO_COLLECTION).initializeUnorderedBulkOp();
			for(let i = 0; i < self.operations.length; i++) {
				let op = self.operations[i];
				bulkOps.find(op.query).upsert().update(op.update);
			}
			self.operations = [];
			bulkOps.execute((err) => {
				if(err) {
					logger.error('Failed to update: %s', err);
				} else {
					logger.info('Update successfully');
				}
				if(end) {
					self.db.close();
					logger.info('Job done');
				} else {
					self.stream.resume();
				}
			});
		}
	}
	onMongoReady() {
		let self = this;

		let count = 0;

		self.stream = db.collection(FROM_COLLECTION).find().stream();
		self.stream.on('data', (data) => {
			++count;
			if(count&5000 === 0) {
				logger.info('Current progress: %s', count);
			}
			self.operations.push({
				query: {
					momoId: data.momoId
				},
				update: {
					$set: {},
					$setOnInsert: {
						type: data.type
					}
				}
			});
			if(self.operations.length >= 1000) {
				self.stream.pause();
				self.emit('MONGO_UPDATE');
			}
		});
		self.stream.on('error', (error) => {
			logger.error(error);
		});
		self.stream.on('end', () => {
			self.emit('MONGO_UPDATE', 'END');
		});
	}
	init() {
		let self = this;
		if(!fs.existsSync(self.logdir)) {
			fs.mkdirSync(self.logdir);
		}

		MongoClient.connect(mongoUrl, (err, db) => {
			if(err) {
				return logger.error('Failed to connect to mongodb: %s', err);
			}
			self.db = db;
			self.db.collection(FROM_COLLECTION).createIndex({momoId: 1, type: 1});
			self.db.collection(TO_COLLECTION).createIndex({momoId: 1, type: 1});
			logger.info('connected to mongodb, init completes...');
			self.emit('MONGO_READY');
		});
	}
	start() {
		this.init();
	}
}

let instance = new Import();
instance.start();
