// 13269908185, newman
// 15669062494, testtest
const fs = require('fs');
const Crawler = require('crawler');
const moment = require('moment');
const EventEmitter = require('events').EventEmitter;
const SESSIONID_LIST = require('./appdata/sessionIds.json');
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/momo.recommend.log`);
const ROUND_TODO = 5;

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
			rateLimit: 5000,
			jQuery: false,
			userAgent: 'MomoChat/7.6 Android/1210 (VTR-AL00; Android 7.0; Gapps 1; zh_CN; 14)'
		});
		this.crawler.on('schedule', (option) => {
			ProxyManager.setOptProxy(option);
			option.headers = option.headers || {};
			let sessionId = null;
			if(option.uri === 'https://live-api.immomo.com/guestv3/mmkit/home/recommend') {
				sessionId = '615D42A2-57B2-CBDB-14D9-270DEFB50E1D_G';
			} else {
				sessionId = this.getSessionId();
			}
			option.headers.Cookie = 'SESSIONID='+sessionId;
			option.limiter = sessionId;
		});
		this.crawler.on('request', (option) => {
		});
		this.crawler.on('drain', () => {
			if(++this.round >= ROUND_TODO) {
				return logger.info('Job done');
			} else {
				logger.info(`Round ${this.round} starts...`);
				this.doneRoom = {};
				this.emit('getRecommend', 0);
			}
		});
		this.on('getRecommend', this.getRecommend.bind(this));
		this.on('getAudience', this.getAudience.bind(this));
		this.on('getProfile', this.getProfile.bind(this));
		this.round = 1;
		this.doneRoom = {};
		this.doneUser = {};
	}

	getRecommend(index) {
		let self = this;
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/guestv3/mmkit/home/recommend',
			priority: 0,
			method: 'POST',
			form: {
				'MDAPI_BackgroundKey':'0',
				'src':'',
				'filtertype':'999',
				'index':index,
				'sex':'ALL',
				'next_time':index === 0 ? '0' : '',
				'lat':'39.879449',	
				'lng':'116.465704'
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

				let roomData = [], hostData = [];
				json.data.lists.forEach(show => {
					if(show.roomid in self.doneRoom) {
						return;
					} else {
						self.doneRoom[show.roomid] = null;
					}
					roomData.push([
							self.round,
							show.roomid,
							show.city,
							show.rtype,
							show.title,
							show.sub_title,
							show.momoid,
							show.people
						].map(text => ((text===undefined?'n/a':text)+'').trim().replace(/[\r\n\t,]/g, ''))
						.join());
					hostData.push([
							show.roomid,
							show.momoid,
							'主播'
						].map(text => ((text===undefined?'n/a':text)+'').trim().replace(/[\r\n\t,]/g, ''))
						.join());

					if(show.momoid) {
						self.emit('getAudience', show.roomid, 0);
					}
				});

				if(roomData.length) {
					fs.appendFileSync(self.resultdir+self.roomResultFile, roomData.join('\n')+'\n');
				}
				if(hostData.length) {
					fs.appendFileSync(self.resultdir+self.userResultFile, hostData.join('\n')+'\n');
				}

				logger.info('%s Got %s/%s live show', logPrefix, roomData.length, json.data.lists.length);
				logger.info('%s, next_flag: %s, next_index: %s', logPrefix, json.data.next_flag, json.data.next_index);

				if(json.data.next_flag && index < 250) {
					self.emit('getRecommend', json.data.next_index);
				}
				done();
			}
		});
	}

	getAudience(roomId, index) {
		let self = this;
		self.crawler.queue({
			uri: 'https://live-api.immomo.com/guestv3/room/ranking/online',
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

				let userData = [];
				json.data.lists.forEach(audience => {
					if(audience.momoid in self.doneUser) {
						return;
					} else {
						self.doneUser[audience.momoid] = null;
					}
					userData.push([
							roomId,
							audience.momoid,
							'观众'
						].map(text => ((text===undefined?'n/a':text)+'').trim().replace(/[\r\n\t,]/g, ''))
						.join());
				});

				if(userData.length) {
					fs.appendFileSync(self.resultdir+self.userResultFile, userData.join('\n')+'\n');
				}

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
			uri: 'https://live-api.immomo.com/guestv3/user/card/lite',
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
				].map(text => ((text===undefined?'n/a':text)+'').trim().replace(/[\r\n\t,]/g, ''))
				.join();

				fs.appendFileSync(self.resultdir+self.userResultFile, profile+'\n');
				logger.info('%s got profile', logPrefix);
				done();
			}
		});
	}

	getSessionId() {
		return SESSIONID_LIST[Math.floor(Math.random()*SESSIONID_LIST.length)];
	}

	init() {
		if(!fs.existsSync(this.logdir)) {
			fs.mkdirSync(this.logdir);
		}
		if(!fs.existsSync(this.resultdir)) {
			fs.mkdirSync(this.resultdir);
		}
		fs.writeFileSync(this.resultdir+this.roomResultFile, '\ufeffround,roomId,city,roomType,roomTitle,roomSubtitle,hostMomoId,audience\n');
		fs.writeFileSync(this.resultdir+this.userResultFile, '\ufeffroomId,momoId,主播/观众\n');
		logger.info('init completes...');
	}

	start() {
		this.init();
		this.emit('getRecommend', 0);
	}
}

let instance = new Momo();
instance.start();
