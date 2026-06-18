import logger_default from "./logger.js";
import { requireObject, requireString } from "./utils.js";
import { randomSafeIntBetween, shuffleInPlace } from "./random.js";
//#region node_modules/@whotracksme/reporting/reporting/src/persisted-counters.js
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
function groupConsecutive(arr) {
	if (arr.length === 0) return [];
	const result = [];
	let current = arr[0];
	let count = 1;
	for (let i = 1; i < arr.length; i += 1) if (arr[i] === current) count += 1;
	else {
		result.push([current, count]);
		current = arr[i];
		count = 1;
	}
	result.push([current, count]);
	return result;
}
var PersistedCounters = class {
	constructor({ name, db }) {
		this.name = requireString(name);
		this.db = requireObject(db);
		this._pendingCounters = /* @__PURE__ */ new Map();
		this._pendingFlush = null;
	}
	count(key) {
		this._pendingCounters.set(key, (this._pendingCounters.get(key) || 0) + 1);
		this._markDirty();
	}
	async clear() {
		this._pendingCounters.clear();
		await this.db.clear();
	}
	async flush() {
		this._pendingFlush = null;
		if (this._pendingCounters.size === 0) return;
		await this.db.transaction({ readonly: false }, async (tx) => {
			const entries = [...this._pendingCounters];
			this._pendingCounters.clear();
			await Promise.all(entries.map(async ([key, inc]) => {
				const oldValue = await tx.get(key) || 0;
				await tx.set(key, oldValue + inc);
			}));
		});
		logger_default.debug("Flushed counters:", this.name);
	}
	/**
	* Draws random samples.
	*
	* Options:
	* - "group": Aggregates identical samples together
	*    Example: ['foo', 'foo', 'bar'] ==> [['foo', 2], ['bar', 1]]
	*/
	async sample(numSamples = 1, { group = false } = {}) {
		await this.flush();
		let samples = [];
		if (numSamples > 0) {
			await this.db.transaction({ readonly: true }, async (tx) => {
				let cursor = await tx.scan();
				let totalSum = 0;
				while (cursor) {
					totalSum += cursor.value;
					cursor = await cursor.next();
				}
				if (totalSum > 0) {
					const picks = [];
					for (let i = 0; i < numSamples; i += 1) picks.push(randomSafeIntBetween(0, totalSum - 1));
					cursor = await tx.scan();
					let currentSum = 0;
					while (cursor && samples.length < numSamples) {
						currentSum += cursor.value;
						for (let i = 0; i < picks.length; i += 1) if (currentSum > picks[i]) {
							samples.push(cursor.key);
							picks[i] = Number.MAX_VALUE;
						}
						cursor = await cursor.next();
					}
				}
			});
			if (group) samples = groupConsecutive(samples);
		}
		return shuffleInPlace(samples);
	}
	async _dumpToMap() {
		await this.flush();
		return this.db._dumpToMap();
	}
	_markDirty() {
		if (this._pendingFlush === null) this._pendingFlush = setTimeout(() => {
			this.flush().catch(logger_default.error);
		}, 200);
	}
	toString() {
		return `PersistedCounters[${this.name}]`;
	}
};
//#endregion
export { PersistedCounters as default };
