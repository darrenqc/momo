
const fs = require('fs');
const Crawler = require('crawler');
const moment = require('moment');
const MongoClient = require('mongodb').MongoClient;
const EventEmitter = require('events').EventEmitter;
const SESSIONID_LIST = require('./appdata/sessionIds.json');
const mongoUrl = 'mongodb://localhost:27017/momo';
const COLLECTION = 'user';
const concurrent = 20;
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.profile.log`);

const ProxyManager = {
	proxies:require('./appdata/proxies.json'),
	idx:0,
	getProxy: function(){
		let cur = this.idx;
		this.idx = (cur+1)%this.proxies.length;
		return this.proxies[cur];
	},
	setOptProxy:function(opt){
		let proxy = this.getProxy();
		opt.proxy = proxy;
		opt.limiter = proxy;
	}
}

class Momo extends EventEmitter {
	constructor() {
		super();
		this.logdir = './log/';
		this.date = moment().format('YYYYMMDD');
		this.db = null;
		this.users = [];
		this.crawler = new Crawler({
			rateLimit: 5000,
			jQuery: false,
			userAgent: 'MomoChat/7.6 Android/1210 (VTR-AL00; Android 7.0; Gapps 1; zh_CN; 14)',
			callback: this.parseProfile.bind(this)
		});
		this.crawler.on('schedule', (option) => {
			ProxyManager.setOptProxy(option);
			option.headers = option.headers || {};
			let sessionId = this.getSessionId();
			option.headers.Cookie = 'SESSIONID='+sessionId;
			option.limiter = sessionId;
		});
		this.crawler.on('drain', () => {
			this.db.close();
			logger.info('Job done');
		});
		this.on('MONGO_READY', this.onMongoReady.bind(this));
		this.on('USERS_READY', this.onUsersReady.bind(this));
	}

	parseProfile(err, res, done) {
		let self = this;
		let momoId = res.options.momoId;
		const logPrefix = `<Profile ${momoId}>`;
		if(err) {
			logger.error('%s Failed to get profile: %s', logPrefix, err);
			self.doUser();
			return done();
		}

		let json = null;
		try {
			json = JSON.parse(res.body);
		} catch(e) {
			logger.error('%s JSON parse failed: %s', logPrefix, res.body);
			self.doUser();
			return done();
		}

		if(json.em !== 'OK') {
			logger.error('%s Status not OK: %s', logPrefix, res.body);
			self.doUser();
			return done();
		}

		json.data = json.data || {};
		['gap_charm','gap_fortune','vip','svip'].forEach(key => {
			json.data[key] = json.data[key] || {};
		});
		['gap_charm','gap_fortune'].forEach(key => {
			if(json.data[key].nextgap) {
				let match = json.data[key].nextgap.match(/^(\d+)万$/);
				if(match) {
					json.data[key].nextgap = parseInt(match[1])*10000;
				}
			}
		});

		let update = {
			nick: json.data.nick,
			sex: json.data.sex || 'U',
			age: parseInt(json.data.age) || null,
			constellation: json.data.constellation || null,
			city: json.data.city || null,
			fans: parseInt(json.data.fansCount) || null
		};
		update[`wealth.${self.date}.charm`] = parseInt(json.data.charm) || null;
		update[`wealth.${self.date}.charmPercent`] = parseInt(json.data.gap_charm.percent) || null;
		update[`wealth.${self.date}.charmGap`] = parseInt(json.data.gap_charm.nextgap) || null;
		update[`wealth.${self.date}.fortune`] = parseInt(json.data.fortune) || null;
		update[`wealth.${self.date}.fortunePercent`] = parseInt(json.data.gap_fortune.percent) || null;
		update[`wealth.${self.date}.fortuneGap`] = parseInt(json.data.gap_fortune.nextgap) || null;

		self.db.collection(COLLECTION).update({momoId: momoId}, {$set: update}, (err) => {
			if(err) {
				logger.error(`${logPrefix} failed to update: ${err}`);
			} else {
				logger.info(`${logPrefix} updated`);
			}
			self.doUser();
			return done();
		});

	}

	getSessionId() {
		return SESSIONID_LIST[Math.floor(Math.random()*SESSIONID_LIST.length)];
	}

	doUser() {
		let self = this;
		let momoId = self.users.shift();
		if(!momoId) {
			return;
		}
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/guestv3/user/card/lite',
			method: 'POST',
			form: {
				'roomid': '1479600263930',
				'remoteid': momoId,
				'src': 'live_onlive_user',
				'lat':'39.879449',
				'lng':'116.465704'
			},
			momoId: momoId
		});
	}

	onUsersReady() {
		let self = this;
		for(let i = 0; i < concurrent; i++) {
			self.doUser();
		}
	}

	onMongoReady() {
		let self = this;
	        let stream = self.db.collection(COLLECTION).find().stream();
		stream.on('data', (data) => {
			self.users.push(data.momoId);
		});
		stream.on('error', (err) => {
			logger.error(err);
		});
		stream.on('end', () => {
			logger.info('All users loaded, total # of users to update: %s', self.users.length);
			self.emit('USERS_READY');
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
			self.db.collection(COLLECTION).createIndex({momoId: 1, type: 1});
			logger.info('connected to mongodb, init completes...');
			self.emit('MONGO_READY');
		});
	}

	start() {
		this.init();
	}
}

let instance = new Momo();
instance.start();
