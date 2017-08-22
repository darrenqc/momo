
const fs = require('fs');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const MongoClient = require('mongodb').MongoClient;
const mongoUrl = 'mongodb://localhost:27017/momo';
const COLLECTION = 'user';
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.stat.log`);
const dir = './result/';

let mode = process.argv.splice(2)[0];
if(!mode || ['week','month'].indexOf(mode) === -1) {
	return logger.info('Usage: node momo.stat.js <mode(week or month)>');
}

let START_TIME = null, END_TIME = null;
let writer_showup = null, writer_fortune = null, writer_charm = null;
if(mode === 'week') {
	START_TIME = moment('2017-08-20', 'YYYY-MM-DD');
	END_TIME = moment('2017-08-27', 'YYYY-MM-DD').startOf('week');
	writer_showup = fs.createWriteStream(`${dir}momo.showup.${moment().subtract(1, 'w').format('YYYY[W]ww')}.csv`);
	writer_fortune = fs.createWriteStream(`${dir}momo.fortune.${moment().subtract(1, 'w').format('YYYY[W]ww')}.csv`);
	writer_charm = fs.createWriteStream(`${dir}momo.charm.${moment().subtract(1, 'w').format('YYYY[W]ww')}.csv`);
} else if(mode === 'month') {
	START_TIME = moment('2017-08', 'YYYY-MM');
	END_TIME = moment().startOf('month');
	writer_showup = fs.createWriteStream(`${dir}momo.showup.${moment().subtract(1, 'M').format('YYYY[M]MM')}.csv`);
	writer_fortune = fs.createWriteStream(`${dir}momo.fortune.${moment().subtract(1, 'M').format('YYYY[M]MM')}.csv`);
	writer_charm = fs.createWriteStream(`${dir}momo.charm.${moment().subtract(1, 'M').format('YYYY[M]MM')}.csv`);
}

let periods = [];
let timepoint = START_TIME.clone();
while(timepoint.isBefore(END_TIME)) {
	if(mode === 'week') {
		periods.push(timepoint.format('YYYY [W]w'));
		timepoint.add(1, 'w');
	} else if(mode === 'month') {
		periods.push(timepoint.format('YYYY-MM'));
		timepoint.add(1, 'M');
	}
}
writer_showup.write('\ufeffmomoId,userType,city,sex,age,constellation,'+periods.join()+'\n');
writer_fortune.write('\ufeffmomoId,userType,city,sex,age,constellation,'+periods.join()+'\n');
writer_charm.write('\ufeffmomoId,userType,city,sex,age,constellation,'+periods.join()+'\n');

logger.info('Periods to calculate: %s', periods);

let event = new EventEmitter();
event.on('MONGO_READY', onMongoReady);
event.on('MONGO_DONE', onMongoEnd);
event.on('ON_DATA', onData);

MongoClient.connect(mongoUrl, (err, db) => {
	if(err) {
		return logger.error('Failed to connect to mongodb: %s', err);
	}
	logger.info('Connected to mongodb, init completes...');
	event.emit('MONGO_READY', db);
});

function onMongoReady(db) {
	let count = 0;
	let stream = db.collection(COLLECTION).find().stream();
	stream.on('data', (data) => {
		++count;
		if(count%5000 === 0) {
			logger.info('current progress: %d', count);
		}
		event.emit('ON_DATA', data);
	});
	stream.on('error', (error) => {
		logger.error(error);
	});
	stream.on('end', () => {
		logger.info('All users loaded');
		event.emit('MONGO_END', db);
	});
}

function onData(data) {
	let fortuneRecords = [], charmRecords = [], showupRecords = [];
	for(let i = 0; i < periods.length; ++i) {
		fortuneRecords.push(getFortune(data));
		charmRecords.push(getCharm(data));
		showupRecords.push(getShowup(data));
	}
	let userInfo = [
		data.momoId,
		data.type,
		data.city,
		data.sex,
		data.age,
		data.constellation
	].map(value => ((value===null?'n/a':value)+'').replace(/\r\n\t,/g, ''));
	writer_showup.write(userInfo.concat(showupRecords).join()+'\n');
	writer_fortune.write(userInfo.concat(fortuneRecords).join()+'\n');
	writer_charm.write(userInfo.concat(charmRecords).join()+'\n');
}

function getFortune(data) {
	let fortune = [];
	if(!('wealth' in data)) {
		for(let i = 0; i < periods.length; i++) {
			fortune.push('n/a');
		}
		return fortune;
	}
	
	let fortuneMap = {};
	Object.keys(data.wealth).forEach(date => {
		let period = null;
		if(mode === 'week') {
			period = moment(date, 'YYYYMMDD').format('YYYY [W]w');
		} else if(mode === 'month') {
			period = moment(date, 'YYYYMMDD').format('YYYY-MM');
		}
		fortuneMap[period] = {
			fortune: data.wealth[date].fortune,
			percent: data.wealth[date].fortunePercent,
			gap: data.wealth[date].fortuneGap
		};
	});

	for(let i = 0; i < periods.length; i++) {
		if(i > 0 && (periods[i] in fortuneMap) && (periods[i-1] in fortuneMap)) {
			fortune.push(calFortuneDiff(fortuneMap[periods[i]],fortuneMap[periods[i-1]]));
		} else if(periods[i] in fortuneMap) {
			fortune.push([
					fortuneMap[periods[i]].fortune,
					fortuneMap[periods[i]].percent,
					fortuneMap[periods[i]].gap
				].join('/'));
		} else {
			fortune.push('n/a');
		}
	}
	return fortune;
}

function getCharm(data) {
	let charm = [];
	if(!('wealth' in data)) {
		for(let i = 0; i < periods.length; i++) {
			charm.push('n/a');
		}
		return charm;
	}
	
	let charmMap = {};
	Object.keys(data.wealth).forEach(date => {
		let period = null;
		if(mode === 'week') {
			period = moment(date, 'YYYYMMDD').format('YYYY [W]w');
		} else if(mode === 'month') {
			period = moment(date, 'YYYYMMDD').format('YYYY-MM');
		}
		charmMap[period] = {
			charm: data.wealth[date].charm,
			percent: data.wealth[date].charmPercent,
			gap: data.wealth[date].charmGap
		};
	});

	for(let i = 0; i < periods.length; i++) {
		if(i > 0 && (periods[i] in charmMap) && (periods[i-1] in charmMap)) {
			charm.push(calCharmDiff(charmMap[periods[i]],charmMap[periods[i-1]]));
		} else if(periods[i] in charmMap) {
			charm.push([
					charmMap[periods[i]].charm,
					charmMap[periods[i]].percent,
					charmMap[periods[i]].gap
				].join('/'));
		} else {
			charm.push('n/a');
		}
	}
	return charm;
}

function calFortuneDiff(fortune_1, fortune_2) {
	return 'diff';
}

function calCharmDiff(charm_1, charm_2) {
	return 'diff';
}

function getShowup(data) {
	let showup = [];
	if(mode === 'week') {
		for(let i = 0; i < periods.length; i++) {
			let count = 0;
			let date = moment(periods[i], 'YYYY [W]w').startOf('week');
			for(let j = 0; j < 7; j++) {
				let monthKey = date.format('[showup_]YYYYMM');
				let dayKey = date.format('D');
				if((monthKey in data) && (dayKey in data[monthKey])) {
					++count;
				}
				date.add(1, 'd');
			}
			showup.push(count);
		}
	} else if(mode === 'month') {
		for(let i = 0; i < periods.length; i++) {
			let key = 'showup_'+moment(periods[i], 'YYYY-MM').format('YYYYMM');
			showup.push((key in data) ? Object.keys(data[key]).length : 0);
		}
	}
	return showup;
}

function onMongoEnd(db) {
	db.close();
}