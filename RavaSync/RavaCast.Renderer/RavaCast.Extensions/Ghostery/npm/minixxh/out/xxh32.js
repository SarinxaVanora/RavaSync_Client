//#region node_modules/minixxh/out/xxh32.js
/**
xxHash32 implementation in pure Javascript

Copyright (C) 2013, Pierre Curto
MIT license
*/
var PRIME32_1 = 2654435761;
var PRIME32_2 = 2246822519;
var PRIME32_3 = 3266489917;
var PRIME32_4 = 668265263;
var PRIME32_5 = 374761393;
var SEED = -1640531535;
function xxh32(input, beg, end) {
	beg >>>= 0;
	end >>>= 0;
	const len = end - beg;
	const end4 = end - 4;
	let p = beg;
	let h32;
	if (len >= 16) {
		const limit16 = end - 16;
		let v1 = 3260726745;
		let v2 = 606290984;
		let v3 = SEED;
		let v4 = SEED - PRIME32_1;
		let lane;
		while (p <= limit16) {
			lane = input[p] | input[p + 1] << 8 | input[p + 2] << 16 | input[p + 3] << 24;
			v1 += Math.imul(lane, PRIME32_2);
			v1 = Math.imul(v1 << 13 | v1 >>> 19, PRIME32_1);
			lane = input[p + 4] | input[p + 5] << 8 | input[p + 6] << 16 | input[p + 7] << 24;
			v2 += Math.imul(lane, PRIME32_2);
			v2 = Math.imul(v2 << 13 | v2 >>> 19, PRIME32_1);
			lane = input[p + 8] | input[p + 9] << 8 | input[p + 10] << 16 | input[p + 11] << 24;
			v3 += Math.imul(lane, PRIME32_2);
			v3 = Math.imul(v3 << 13 | v3 >>> 19, PRIME32_1);
			lane = input[p + 12] | input[p + 13] << 8 | input[p + 14] << 16 | input[p + 15] << 24;
			v4 += Math.imul(lane, PRIME32_2);
			v4 = Math.imul(v4 << 13 | v4 >>> 19, PRIME32_1);
			p += 16;
		}
		h32 = (v1 << 1 | v1 >>> 31) + (v2 << 7 | v2 >>> 25) + (v3 << 12 | v3 >>> 20) + (v4 << 18 | v4 >>> 14);
	} else h32 = -1265770142;
	h32 += len;
	while (p <= end4) {
		h32 += Math.imul(input[p] | input[p + 1] << 8 | input[p + 2] << 16 | input[p + 3] << 24, PRIME32_3);
		h32 = Math.imul(h32 << 17 | h32 >>> 15, PRIME32_4);
		p += 4;
	}
	while (p < end) {
		h32 += Math.imul(input[p++], PRIME32_5);
		h32 = Math.imul(h32 << 11 | h32 >>> 21, PRIME32_1);
	}
	h32 ^= h32 >>> 15;
	h32 = Math.imul(h32, PRIME32_2);
	h32 ^= h32 >>> 13;
	h32 = Math.imul(h32, PRIME32_3);
	h32 ^= h32 >>> 16;
	return h32 >>> 0;
}
//#endregion
export { xxh32 };
