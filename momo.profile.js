
const fs = require('fs');
const Crawler = require('crawler');
const moment = require('moment');
const MongoClient = require('mongodb').MongoClient;
const EventEmitter = require('events').EventEmitter;
const SESSIONID_LIST = require('./appdata/sessionIds.json');
const mongoUrl = 'mongodb://localhost:27017/momo';
let COLLECTION = 'sample';
if(__dirname.indexOf('_contrast') > -1) {
	COLLECTION = 'sample_contrast';
}
const concurrent = 60;
const RETRIES = 5;
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
		let user = res.options.user;
		const logPrefix = `<Profile ${user.momoId}>`;
		if(err) {
			logger.error('%s Failed to get profile: %s', logPrefix, err);
			self.doUser(user);
			return done();
		}

		let json = null;
		try {
			json = JSON.parse(res.body);
		} catch(e) {
			logger.error('%s JSON parse failed: %s', logPrefix, res.body);
			self.doUser(user);
			return done();
		}

		if(json.em !== 'OK') {
			logger.error('%s Status not OK: %s', logPrefix, res.body);
			self.doUser(user);
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
			json.data[key].percent = isNaN(parseInt(json.data[key].percent)) ? null : parseInt(json.data[key].percent);
			json.data[key].nextgap = isNaN(parseInt(json.data[key].nextgap)) ? null : parseInt(json.data[key].nextgap);
		});

		['charm','fortune'].forEach(key => {
			json.data[key] = isNaN(parseInt(json.data[key])) ? null : parseInt(json.data[key]);
		});

		if(user.retries > 0) {
			if(user.lastCharm !== null) {
				if(user.lastCharm > 0 && (json.data.charm === null || json.data.charm === 0)) {
					logger.warn('user %s add 1 to retries, %s retries left got invalid charm, %s', user.momoId, user.retries, res.body);
					user.retries++;
					self.doUser(user);
					return done();
				}
				if(json.data.charm === null || json.data.charm < user.lastCharm) {
					logger.warn('user %s %s retries left got invalid charm, %s', user.momoId, user.retries, res.body);
					self.doUser(user);
					return done();
				}
			}
			if(user.lastFortune !== null) {
				if(user.lastFortune > 0 && (json.data.fortune === null || json.data.fortune === 0)) {
					logger.warn('user %s add 1 to retries, %s retries left got invalid fortune, %s', user.momoId, user.retries, res.body);
					user.retries++;
					self.doUser(user);
					return done();
				}
				if(json.data.fortune === null || json.data.fortune < user.lastFortune) {
					logger.warn('user %s %s retries left got invalid fortune, %s', user.momoId, user.retries, res.body);
					self.doUser(user);
					return done();
				}
			}

			if(user.lastCharm === null && user.lastFortune === null) {
				if(json.data.fortune === null || json.data.fortune === 0 || json.data.charm === null || json.data.fortune === 0) {
					logger.warn('user %s %s retries left, got null, %s', user.momoId, user.retries, res.body);
					self.doUser(user);
					return done();
				}
			}
		} else {
			logger.warn('user %s no retries left, compromise to %s', user.momoId, res.body);
		}

		let update = {
			nick: json.data.nick,
			sex: json.data.sex || 'U',
			age: parseInt(json.data.age) || null,
			constellation: json.data.constellation || null,
			city: json.data.city || null,
			fans: parseInt(json.data.fansCount) || null
		};
		update[`wealth.${self.date}.charm`] = json.data.charm;
		update[`wealth.${self.date}.charmPercent`] = json.data.gap_charm.percent;
		update[`wealth.${self.date}.charmGap`] = json.data.gap_charm.nextgap;
		update[`wealth.${self.date}.fortune`] = json.data.fortune;
		update[`wealth.${self.date}.fortunePercent`] = json.data.gap_fortune.percent;
		update[`wealth.${self.date}.fortuneGap`] = json.data.gap_fortune.nextgap;

		self.db.collection(COLLECTION).update({momoId: user.momoId}, {$set: update}, (err) => {
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

	doUser(user) {
		let self = this;
		if(!user) {
			user = self.users.shift();
		}
		if(!user) {
			return;
		}
		if(user.retries-- <= 0) {
			return self.doUser();
		}
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/guestv3/user/card/lite',
			method: 'POST',
			form: {
				'roomid': '1479600263930',
				'remoteid': user.momoId,
				'src': 'live_onlive_user',
				'lat':'39.879449',
				'lng':'116.465704'
			},
			user: user
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
			let lastFortune = null, lastCharm = null;
			if('wealth' in data) {
				let status = data.wealth[Object.keys(data.wealth)[Object.keys(data.wealth).length-1]];
				lastFortune = status.fortune;
				lastCharm = status.charm;
			}
			self.users.push({
				momoId: data.momoId,
				lastFortune: lastFortune,
				lastCharm: lastCharm,
				retries: RETRIES
			});
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
