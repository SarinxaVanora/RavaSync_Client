//#region node_modules/@whotracksme/reporting/reporting/src/random.js
/**
* WhoTracks.Me
* https://whotracks.me/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
function random() {
	return random53Bit() / 2 ** 53;
}
function random53Bit() {
	const values = crypto.getRandomValues(new Uint32Array(2));
	return 2 ** 32 * (values[0] & 2097151) + values[1];
}
function random32Bit() {
	return crypto.getRandomValues(new Uint32Array(1))[0];
}
function randomSafeInteger() {
	const values = crypto.getRandomValues(new Uint32Array(2));
	const x = 2 ** 32 * (values[0] & 2097151) + values[1];
	const positive = values[0] & 2 ** 22;
	if (x === 0 && !positive) return randomSafeInteger();
	return positive ? x : -x;
}
/**
* Returns a floating point (not integer!) that lies between the boundaries.
*
* Hint: if you need an unbiased integer, use "randomSafeIntBetween".
*/
function randomBetween(minInclusive, maxInclusive) {
	if (!Number.isFinite(minInclusive)) throw new Error(`minInclusive=${minInclusive} must be a finite number`);
	if (!Number.isFinite(maxInclusive)) throw new Error(`maxInclusive=${maxInclusive} must be a finite number`);
	if (maxInclusive < minInclusive) throw new Error(`maxInclusive=${maxInclusive} must be at least minInclusive=${minInclusive}`);
	const diff = maxInclusive - minInclusive;
	if (diff === 0) return minInclusive;
	return minInclusive + random() * diff;
}
/**
* Return an unbiased random integer that lies between the boundaries (both inclusive).
*/
function randomSafeIntBetween(minInclusive, maxInclusive) {
	if (!Number.isSafeInteger(minInclusive)) throw new Error(`minInclusive=${minInclusive} must be a safe integer`);
	if (!Number.isSafeInteger(maxInclusive)) throw new Error(`maxInclusive=${maxInclusive} must be a safe integer`);
	if (maxInclusive < minInclusive) throw new Error(`maxInclusive=${maxInclusive} must be at least minInclusive=${minInclusive}`);
	const diff = maxInclusive - minInclusive;
	if (diff === 0) return minInclusive;
	if (Number.isSafeInteger(diff)) {
		const randGen = diff < 2 ** 32 ? random32Bit : random53Bit;
		let nextPow2 = 1;
		while (diff >= nextPow2) nextPow2 *= 2;
		for (let attempts = 0; attempts < 1e3; attempts += 1) {
			const x = randGen() % nextPow2;
			if (x <= diff) return minInclusive + x;
		}
	} else for (let attempts = 0; attempts < 1e3; attempts += 1) {
		const x = randomSafeInteger();
		if (x >= minInclusive && x <= maxInclusive) return x;
	}
	throw new Error(`Internal error: unable to pick a random number between ${minInclusive} and ${maxInclusive}`);
}
function shuffleInPlace(array) {
	for (let i = array.length - 1; i > 0; i -= 1) {
		const j = randomSafeIntBetween(0, i);
		[array[i], array[j]] = [array[j], array[i]];
	}
	return array;
}
//#endregion
export { random as default, random32Bit, randomBetween, randomSafeIntBetween, shuffleInPlace };
