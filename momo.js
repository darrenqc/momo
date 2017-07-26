// 13269908185, newman
// 15669062494, testtest
const fs = require('fs');
const Crawler = require('crawler');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.log`);

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
		this.resultdir = './result/';
		this.roomResultFile = `momo.room.${moment().format('YYYY-MM-DD')}.csv`;
		this.userResultFile = `momo.user.${moment().format('YYYY-MM-DD')}.csv`;
		this.crawler = new Crawler({
			rateLimit: 2000,
			jQuery: false,
			userAgent: 'MomoChat/8.0.1 Android/1436 (HUAWEIMT7-TL00; Android 4.4.2; Gapps 1; zh_CN; 14)'
		});
		this.crawler.on('schedule', (option) => {
			// ProxyManager.setOptProxy(option);
		});
		this.crawler.on('request', (option) => {
			option.headers = option.headers || {};
			option.headers.Cookie = 'SESSIONID='+this.getSessionId();
			logger.info(option.headers.Cookie);
		});
		this.crawler.on('drain', () => {
			logger.info('Job done');
		});
		this.on('getRecommend', this.getRecommend.bind(this));
		this.on('getAudience', this.getAudience.bind(this));
		this.on('getProfile', this.getProfile.bind(this));
	}

	getRecommend(index) {
		let self = this;
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/v3/mmkit/home/recommend',
			method: 'POST',
			form: {
				'next_time':index === 0 ? '0' : '',
				'index':index,
				'lat':'39.879449',
				'lng':'116.465704',
				'sex':'ALL',
				'src':'',
				'filtertype':'999',
				'MDAPI_BackgroundKey':'1'
			},
			callback: (err, res, done) => {
				const logPrefix = `<Recommend index ${index}>`
				if(err) {
					logger.error('%s Failed to fetch recommended live shows: %s', logPrefix, err);
					return done();
				}

				let json = null;
				try {
					json = JSON.parse(res.body);
				} catch(e) {
					logger.error('%s JSON parse failed: %s', logPrefix, res.body);
					return done();
				}

				if(json.em !== 'OK') {
					logger.error('%s Status not OK: %s', logPrefix, res.body);
					return done();
				}

				json.data = json.data || {};
				json.data.lists = json.data.lists || [];

				let roomData = [];
				json.data.lists.forEach(show => {
					roomData.push([
							show.roomid,
							show.city,
							show.rtype,
							show.title,
							show.sub_title,
							show.momoid,
							show.people
						].map(text => ((text||'n/a')+'').trim().replace(/[\r\n\t,]/g, ''))
						.join());

					if(show.momoid) {
						self.emit('getAudience', show.roomid, 0);
						self.emit('getProfile', show.roomid, show.momoid, 'host');
					}
				});

				if(roomData.length) {
					fs.appendFileSync(self.resultdir+self.roomResultFile, roomData.join('\n')+'\n');
				}

				logger.info('%s Got %s live show', logPrefix, json.data.lists.length);
				logger.info('%s, next_flag: %s, next_index: %s', logPrefix, json.data.next_flag, json.data.next_index);

				if(json.data.next_flag) {
					self.emit('getRecommend', json.data.next_index);
				}
				done();
			}
		});
	}

	getAudience(roomId, index) {
		let self = this;
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/v3/room/ranking/online',
			method: 'POST',
			form: {
				'roomid': roomId,
				'index': index,
				'lat':'39.879449',
				'lng':'116.465704'
			},
			callback: (err, res, done) => {
				const logPrefix = `<Audience room ${roomId} index ${index}>`;
				if(err) {
					logger.error('%s Failed to get audience: %s', logPrefix, err);
					return done();
				}

				let json = null;
				try {
					json = JSON.parse(res.body);
				} catch(e) {
					logger.error('%s JSON parse failed: %s', logPrefix, res.body);
					return done();
				}

				if(json.em !== 'OK') {
					logger.error('%s Status not OK: %s', logPrefix, res.body);
					return done();
				}

				json.data = json.data || {};
				json.data.lists = json.data.lists || [];

				json.data.lists.forEach(audience => {
					self.emit('getProfile', roomId, audience.momoid, 'audience');
				});

				logger.info('%s got %s audience', logPrefix, json.data.lists.length);
				logger.info('%s, has_next: %s, next_index: %s', logPrefix, json.data.has_next, json.data.next_index);

				if(json.data.has_next) {
					self.emit('getAudience', roomId, json.data.next_index);
				}
				done();
			}
		});
	}

	getProfile(roomId, momoId, userType) {
		let self = this;
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/v3/user/card/lite',
			method: 'POST',
			form: {
				'roomid': roomId,
				'remoteid': momoId,
				'src': 'live_onlive_user',
				'lat':'39.879449',
				'lng':'116.465704'
			},
			callback: (err, res, done) => {
				const logPrefix = `<Profile room ${roomId} ${userType} ${momoId}>`;
				if(err) {
					logger.error('%s Failed to get profile: %s', logPrefix, err);
					return done();
				}

				let json = null;
				try {
					json = JSON.parse(res.body);
				} catch(e) {
					logger.error('%s JSON parse failed: %s', logPrefix, res.body);
					return done();
				}

				if(json.em !== 'OK') {
					logger.error('%s Status not OK: %s', logPrefix, res.body);
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

				let profile = [
					json.data.momoid,
					json.data.nick,
					roomId,
					userType,
					json.data.is_zhubo,
					json.data.sex,
					json.data.age,
					json.data.constellation,
					json.data.city,
					json.data.fansCount,
					json.data.fans_num,
					json.data.charm,
					json.data.fortune,
					json.data.gap_charm.percent,
					json.data.gap_charm.nextgap,
					json.data.gap_fortune.percent,
					json.data.gap_fortune.nextgap,
					json.data.vip.valid,
					json.data.vip.active_level,
					json.data.svip.valid,
					json.data.svip.active_level
				].map(text => ((text||'n/a')+'').trim().replace(/[\r\n\t,]/g, ''))
				.join();

				fs.appendFileSync(self.resultdir+self.userResultFile, profile+'\n');
				logger.info('%s got profile', logPrefix);
				done();
			}
		});
	}

	getSessionId() {
		const list = [
			'540E04BB-7972-1EE8-A96E-969FFAF7A1C7',
			'81000CF9-42BE-D970-A95E-6C43E2F1969C'
		]
		return list[Math.floor(Math.random()*list.length)];
	}

	init() {
		if(!fs.existsSync(this.logdir)) {
			fs.mkdirSync(this.logdir);
		}
		if(!fs.existsSync(this.resultdir)) {
			fs.mkdirSync(this.resultdir);
		}
		fs.writeFileSync(this.resultdir+this.roomResultFile, '\ufeffroomId,city,roomType,roomTitle,roomSubtitle,hostMomoId,audience\n')
		fs.writeFileSync(this.resultdir+this.userResultFile, '\ufeffmomoId,nick,roomId,userType,isZhubo,sex,age,constellation,city,fans,fansGroup,charm,fortune,charmPercent,charmGap,fortunePercent,fortuneGap,vipValid,vipLevel,svipValid,svipLevel\n')
		logger.info('init completes...');
	}

	start() {
		this.init();
		this.emit('getRecommend', 0);
	}
}

let instance = new Momo();
instance.start();
