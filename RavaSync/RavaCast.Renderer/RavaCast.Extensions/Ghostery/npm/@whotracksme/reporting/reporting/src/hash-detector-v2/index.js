import prob_default from "./prob.js";
//#region node_modules/@whotracksme/reporting/reporting/src/hash-detector-v2/index.js
var RE_PURE_NUMBERS = /^\d+$/;
var RE_RESOLUTION = /^\d+x\d+$/;
var RE_UK_POSTCODE = /^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$/i;
var RE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
var RE_PERCENT_ENCODED = /%[0-9A-Fa-f]{2}/;
var HashProb = class {
	constructor() {
		this.probHashLogM = prob_default.logM;
		this.classifierWeights = prob_default.classifierWeights;
		this.probHashChars = {};
		"abcdefghijklmnopqrstuvwxyz1234567890.- ".split("").forEach((e, idx) => {
			this.probHashChars[e] = idx;
		});
	}
	isHashProb(str) {
		if (!this.probHashLogM) return 0;
		let logProb = 0;
		let transC = 0;
		str = str.toLowerCase().replace(/[^a-z0-9.\- ]/g, "");
		for (let i = 0; i < str.length - 1; i += 1) {
			const pos1 = this.probHashChars[str[i]];
			const pos2 = this.probHashChars[str[i + 1]];
			logProb += this.probHashLogM[pos1][pos2];
			transC += 1;
		}
		if (transC > 0) return Math.exp(logProb / transC);
		return Math.exp(logProb);
	}
	isHash(str) {
		if (str.length < 6) return false;
		if (str.includes("%") && RE_PERCENT_ENCODED.test(str)) {
			try {
				str = decodeURIComponent(str);
			} catch (e) {}
			if (str.length < 6) return false;
		}
		if (RE_PURE_NUMBERS.test(str)) return false;
		if (RE_RESOLUTION.test(str)) return false;
		if (RE_UK_POSTCODE.test(str)) return false;
		if (RE_UUID.test(str)) return true;
		const bigramProb = this.isHashProb(str);
		const features = extractFeatures(str, bigramProb);
		const w = this.classifierWeights;
		const score = w[0] + w[1] * features.bigramProb + w[2] * features.entropy + w[3] * features.hexRatio + w[4] * features.vowelRatio + w[5] * features.transitions;
		return 1 / (1 + Math.exp(-score)) > (w[6] ?? .5);
	}
};
/**
* Extract all 5 features from a string in a single pass.
* All features are O(n) in string length.
*/
function extractFeatures(str, bigramProb) {
	const lower = str.toLowerCase();
	const len = lower.length;
	const freq = {};
	let hexCount = 0;
	let vowelCount = 0;
	let alphaCount = 0;
	let transitionCount = 0;
	let prevClass = -1;
	for (let i = 0; i < len; i++) {
		const c = lower[i];
		const code = lower.charCodeAt(i);
		freq[c] = (freq[c] || 0) + 1;
		if (code >= 48 && code <= 57 || code >= 97 && code <= 102) hexCount++;
		if (code >= 97 && code <= 122) {
			alphaCount++;
			if (c === "a" || c === "e" || c === "i" || c === "o" || c === "u") vowelCount++;
		}
		let curClass;
		if (code >= 97 && code <= 122) curClass = 0;
		else if (code >= 48 && code <= 57) curClass = 1;
		else curClass = -1;
		if (prevClass >= 0 && curClass >= 0 && prevClass !== curClass) transitionCount++;
		if (curClass >= 0) prevClass = curClass;
	}
	let entropy = 0;
	for (const c in freq) {
		const p = freq[c] / len;
		entropy -= p * Math.log2(p);
	}
	return {
		bigramProb,
		entropy,
		hexRatio: len > 0 ? hexCount / len : 0,
		vowelRatio: alphaCount > 0 ? vowelCount / alphaCount : 0,
		transitions: len > 1 ? transitionCount / (len - 1) : 0
	};
}
//#endregion
export { HashProb };
