
const fs = require('fs');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const MongoClient = require('mongodb').MongoClient;
const mongoUrl = 'mongodb://localhost:27017/momo';
const WHOLE_COLLECTION = 'user';
const SAMPLE_COLLECTION = 'sample';
const SAMPLE_INTERVAL = 4;
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.sample.log`);

let event = new EventEmitter();
event.on('MONGO_READY', onMongoReady);
event.on('MONGO_END', onMongoEnd);
event.on('ON_DATA', onData);

let mode, distribution, sampleIds = {};
if(!fs.existsSync('./result/momoId.distribution.json')) {
	distribution = {
		host: [],
		user: {}
	};
	mode = 'distribution';
} else {
	mode = 'sample';
}
logger.info('current mode is <%s>', mode);

if(mode === 'sample') {
	distribution = JSON.parse(fs.readFileSync('./result/momoId.distribution.json').toString());
	distribution.host.forEach(momoId => {
		sampleIds[momoId] = null;
	});
	Object.keys(distribution.user).forEach(fortuneLevel => {
		if(fortuneLevel === 'null') {
			return;
		}
		for(let i = 0; i < distribution.user[fortuneLevel].length; i+=SAMPLE_INTERVAL) {
			sampleIds[distribution.user[fortuneLevel][i]] = null;
		}
	});
	logger.info('Total # of sampleIds: %s', Object.keys(sampleIds).length);
}

let countToInsert = 0;

MongoClient.connect(mongoUrl, (err, db) => {
	if(err) {
		return logger.error('Failed to connect to mongodb: %s', err);
	}
	logger.info('Connected to mongodb, init completes...');
	event.emit('MONGO_READY', db);
});

function onMongoReady(db) {
	if(mode === 'sample') {
		db.collection(SAMPLE_COLLECTION).createIndex({momoId:1,type:1},{unique:true});
	}

	let count = 0;
	let stream = db.collection(WHOLE_COLLECTION).find().stream();
	stream.on('data', (data) => {
		++count;
		if(count%5000 === 0) {
			logger.info('current progress: %d', count);
		}
		if(mode === 'sample') {
			if(data.momoId in sampleIds) {
				++countToInsert;
				sanitize(data);
				db.collection(SAMPLE_COLLECTION).insertOne(data, (err, res) => {
					--countToInsert;
					if(err) {
						logger.error('Failed to insert momoId=%s, %s', data.momoId, err);
					}
				});
			}
		} else {
			event.emit('ON_DATA', data);
		}
		
	});
	stream.on('error', (error) => {
		logger.error(error);
	});
	stream.on('end', () => {
		logger.info('All users loaded');
		if(mode === 'distribution') {
			fs.writeFileSync('./result/momoId.distribution.json', JSON.stringify(distribution));
		}
		setTimeout(function() {
			event.emit('MONGO_END', db);
		}, 100000);
	});
}

function sanitize(data) {
	if('wealth' in data) {
		Object.keys(data.wealth).forEach(date => {
			if(data.wealth[date].fortune === null && data.wealth[date].fortunePercent !== null) {
				data.wealth[date].fortune = 0;
			}
			if(data.wealth[date].charm === null && data.wealth[date].charmPercent !== null) {
				data.wealth[date].charm = 0;
			}
		});
	}
}

function onData(data) {
	sanitize(data);
	if(data.type === '主播') {
		distribution.host.push(data.momoId);
	} else {
		let fortuneLevel = ('wealth' in data) ? data.wealth[Object.keys(data.wealth)[Object.keys(data.wealth).length-1]].fortune : 'null';
		if(!(fortuneLevel in distribution.user)) {
			distribution.user[fortuneLevel] = [];
		}
		let index = binarySearch(0, distribution.user[fortuneLevel].length, distribution.user[fortuneLevel], data.momoId);
		distribution.user[fortuneLevel].splice(index, 0, data.momoId);
	}
}

function binarySearch(start, end, array, value) {
	if(start === end) {
		if(array[start] && value > array[start]) {
			return start+1;
		} else {
			return start;
		}
	}
	if(start > end) {
		return start;
	}
	let half = start + Math.floor((end-start)/2);
	if(value < array[half]) {
		return binarySearch(start, half-1, array, value);
	}
	if(value > array[half]) {
		return binarySearch(half+1, end, array, value);
	}
	if(value == array[half]) {
		return half;
	}
}

function onMongoEnd(db) {
	if(countToInsert === 0) {
		db.close();
		logger.info('Job done');
	} else {
		setTimeout(function() {
			event.emit('MONGO_END', db);
		}, 10000);
	}
}
