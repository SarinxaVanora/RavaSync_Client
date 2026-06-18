//#region node_modules/minixxh/out/xxh32d64.js
/**
xxHash32 dual-lane 64-bit hash in pure Javascript
*/
var PRIME32_1 = 2654435761;
var PRIME32_2 = 2246822519;
var PRIME32_3 = 3266489917;
var PRIME32_4 = 668265263;
var PRIME32_5 = 374761393;
var XXH32D64_SEED_HIGH = -1640531535;
var XXH32D64_SEED_LOW = -2048144777;
function xxh32d64(input, beg, end) {
	beg >>>= 0;
	end >>>= 0;
	const len = end - beg;
	const end4 = end - 4;
	let p = beg;
	let h32High;
	let h32Low;
	let lane;
	if (len >= 16) {
		const limit16 = end - 16;
		let highV1 = 3260726745;
		let highV2 = 606290984;
		let highV3 = XXH32D64_SEED_HIGH;
		let highV4 = XXH32D64_SEED_HIGH - PRIME32_1;
		let lowV1 = 2853113503;
		let lowV2 = 198677742;
		let lowV3 = XXH32D64_SEED_LOW;
		let lowV4 = XXH32D64_SEED_LOW - PRIME32_1;
		while (p <= limit16) {
			lane = input[p] | input[p + 1] << 8 | input[p + 2] << 16 | input[p + 3] << 24;
			highV1 += Math.imul(lane, PRIME32_2);
			highV1 = Math.imul(highV1 << 13 | highV1 >>> 19, PRIME32_1);
			lowV1 += Math.imul(lane, PRIME32_2);
			lowV1 = Math.imul(lowV1 << 13 | lowV1 >>> 19, PRIME32_1);
			lane = input[p + 4] | input[p + 5] << 8 | input[p + 6] << 16 | input[p + 7] << 24;
			highV2 += Math.imul(lane, PRIME32_2);
			highV2 = Math.imul(highV2 << 13 | highV2 >>> 19, PRIME32_1);
			lowV2 += Math.imul(lane, PRIME32_2);
			lowV2 = Math.imul(lowV2 << 13 | lowV2 >>> 19, PRIME32_1);
			lane = input[p + 8] | input[p + 9] << 8 | input[p + 10] << 16 | input[p + 11] << 24;
			highV3 += Math.imul(lane, PRIME32_2);
			highV3 = Math.imul(highV3 << 13 | highV3 >>> 19, PRIME32_1);
			lowV3 += Math.imul(lane, PRIME32_2);
			lowV3 = Math.imul(lowV3 << 13 | lowV3 >>> 19, PRIME32_1);
			lane = input[p + 12] | input[p + 13] << 8 | input[p + 14] << 16 | input[p + 15] << 24;
			highV4 += Math.imul(lane, PRIME32_2);
			highV4 = Math.imul(highV4 << 13 | highV4 >>> 19, PRIME32_1);
			lowV4 += Math.imul(lane, PRIME32_2);
			lowV4 = Math.imul(lowV4 << 13 | lowV4 >>> 19, PRIME32_1);
			p += 16;
		}
		h32High = (highV1 << 1 | highV1 >>> 31) + (highV2 << 7 | highV2 >>> 25) + (highV3 << 12 | highV3 >>> 20) + (highV4 << 18 | highV4 >>> 14);
		h32Low = (lowV1 << 1 | lowV1 >>> 31) + (lowV2 << 7 | lowV2 >>> 25) + (lowV3 << 12 | lowV3 >>> 20) + (lowV4 << 18 | lowV4 >>> 14);
	} else {
		h32High = -1265770142;
		h32Low = -1673383384;
	}
	h32High += len;
	h32Low += len;
	while (p <= end4) {
		lane = input[p] | input[p + 1] << 8 | input[p + 2] << 16 | input[p + 3] << 24;
		h32High += Math.imul(lane, PRIME32_3);
		h32High = Math.imul(h32High << 17 | h32High >>> 15, PRIME32_4);
		h32Low += Math.imul(lane, PRIME32_3);
		h32Low = Math.imul(h32Low << 17 | h32Low >>> 15, PRIME32_4);
		p += 4;
	}
	while (p < end) {
		lane = input[p++];
		h32High += Math.imul(lane, PRIME32_5);
		h32High = Math.imul(h32High << 11 | h32High >>> 21, PRIME32_1);
		h32Low += Math.imul(lane, PRIME32_5);
		h32Low = Math.imul(h32Low << 11 | h32Low >>> 21, PRIME32_1);
	}
	h32High ^= h32High >>> 15;
	h32High = Math.imul(h32High, PRIME32_2);
	h32High ^= h32High >>> 13;
	h32High = Math.imul(h32High, PRIME32_3);
	h32High ^= h32High >>> 16;
	h32Low ^= h32Low >>> 15;
	h32Low = Math.imul(h32Low, PRIME32_2);
	h32Low ^= h32Low >>> 13;
	h32Low = Math.imul(h32Low, PRIME32_3);
	h32Low ^= h32Low >>> 16;
	return BigInt(h32High >>> 0) << 32n | BigInt(h32Low >>> 0);
}
//#endregion
export { xxh32d64 };
