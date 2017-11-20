const charmLevelSum = require('./appdata/charmLevelSum.json');
const charmLevelGap = require('./appdata/charmLevelGap.json');
const fortuneLevelGap = require('./appdata/fortuneLevelGap.json');
const fortuneLevelSum = require('./appdata/fortuneLevelSum.json');

module.exports = {
	point2Charm: point2Charm,
	point2Fortune: point2Fortune
}

function point2Charm(point) {
	if (isNaN(point)) {
		return {
			charm: 0,
			charmPercent: null,
			charmGap: null
		}
	}
	let i = 0;
	for (; i < charmLevelSum.length; i++) {
		if (point < charmLevelSum[i]) {
			i--;
			break;
		}
	}
	i = Math.min(i, charmLevelSum.length - 1);
	let charmGap = charmLevelGap[i] - (point - charmLevelSum[i]);
	let charmPercent = Math.floor((charmGap/charmLevelGap[i])*100);
	return {
		charm: i,
		charmPercent: charmPercent,
		charmGap: Math.floor(charmGap)
	}
}

function point2Fortune(point) {
	if (isNaN(point)) {
		return {
			fortune: 0,
			fortunePercent: null,
			fortuneGap: null
		}
	}
	let i = 0;
	for (; i < fortuneLevelSum.length; i++) {
		if (point < fortuneLevelSum[i]) {
			i--;
			break;
		}
	}
	i = Math.min(i, fortuneLevelSum.length - 1);
	let fortuneGap = fortuneLevelGap[i] - (point - fortuneLevelSum[i]);
	let fortunePercent = Math.floor((fortuneGap/fortuneLevelGap[i])*100);
	return {
		fortune: i,
		fortunePercent: fortunePercent,
		fortuneGap: Math.floor(fortuneGap)
	}
}

