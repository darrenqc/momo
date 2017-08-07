
const fs = require('fs');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const Byline = require('line-by-line');
const MongoClient = require('mongodb').MongoClient;
const mongoUrl = 'mongodb://localhost:27017/momo';
const COLLECTION = 'user';
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/import_user.log`);

class Import extends EventEmitter {
	constructor() {
		super();
		this.logdir = './log/';
		this.date = null;
		this.reader = null;
		this.db = null;
		this.operations = [];
		this.on('MONGO_READY', this.onMongoReady.bind(this));
		this.on('MONGO_UPDATE', this.onMongoUpdate.bind(this));
	}
	onMongoUpdate(end) {
		let self = this;
		if(!self.operations.length) {
			if(end) {
				self.reader.close();
				self.db.close();
				logger.info('Job done');
			} else {
				self.reader.resume();
			}
		} else {
			let bulkOps = self.db.collection(COLLECTION).initializeUnorderedBulkOp();
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
					self.reader.close();
					self.db.close();
					logger.info('Job done');
				} else {
					self.reader.resume();
				}
			});
		}
	}
	onMongoReady() {
		let self = this;
		self.reader = new Byline(`./result/momo.user.${self.date.format('YYYY-MM-DD')}.csv`);
		self.reader.on('line', (line) => {
			let user = self.parse(line);
			if(user === null) {
				return;
			}
			let key = `showup_${self.date.format('YYYYMM')}.${self.date.format('D')}`;
			let set = {};
			set[key] = null;
			self.operations.push({
				query: {
					momoId: user.momoId
				},
				update: {
					$set: set,
					$setOnInsert: {
						type: user.type
					}
				}
			});
			if(self.operations.length >= 1000) {
				self.reader.pause();
				self.emit('MONGO_UPDATE');
			}
		});
		self.reader.on('error', (err) => {
			logger.error(err);
		});
		self.reader.on('end', () => {
			self.emit('MONGO_UPDATE', 'END');
		});
	}
	parse(line) {
		let vals = line.trim().split(',');
		if(vals.length !== 3) {
			return null;
		}
		if(isNaN(vals[1]) || ['主播','观众'].indexOf(vals[2]) === -1) {
			return null;
		}
		return {
			momoId: parseInt(vals[1]),
			type: vals[2]
		}
	}
	init() {
		let self = this;
		if(!fs.existsSync(self.logdir)) {
			fs.mkdirSync(self.logdir);
		}
		self.date = process.argv.splice(2)[0] || moment().subtract(1, 'd').format('YYYY-MM-DD');
		if(!/^\d{4}-\d{2}-\d{2}$/.test(self.date)) {
			return logger.error('Usage: node import_user.js <date(default to yesterday)> e.g. node import_user.js 2017-07-01');
		}
		self.date = moment(self.date, 'YYYY-MM-DD');

		MongoClient.connect(mongoUrl, (err, db) => {
			if(err) {
				return logger.error('Failed to connect to mongodb: %s', err);
			}
			self.db = db;
			self.db.collection(COLLECTION).createIndex({momoId: 1, type: 1});
			logger.info('%s, connected to mongodb, init completes...', self.date.format('YYYY-MM-DD'));
			self.emit('MONGO_READY');
		});
	}
	start() {
		this.init();
	}
}

let instance = new Import();
instance.start();
