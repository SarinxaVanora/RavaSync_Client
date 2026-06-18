import ChromeStorageMap from "../../utils/chrome-storage-map.js";
//#region node_modules/@whotracksme/reporting/reporting/src/request/steps/token-telemetry/cached-entry-pipeline.js
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
var CACHE_TTL = 2880 * 60 * 1e3;
/**
* Abstract part of token/key processing logic.
*/
var CachedEntryPipeline = class {
	constructor(params) {
		this.name = params.name;
		this.db = params.db;
		this.trustedClock = params.trustedClock;
		this.cache = new ChromeStorageMap({
			storageKey: `wtm-request-reporting:token-telemetry:${this.name}`,
			ttlInMs: CACHE_TTL
		});
		this.primaryKey = params.primaryKey;
		this.options = params.options;
		this.sendMessage = params.sendMessage;
		this.batchInterval = params.batchInterval;
		this.batchLimit = params.batchLimit;
	}
	get(key) {
		const entry = this.getFromCache(key);
		entry.count += 1;
		return entry;
	}
	/**
	* Loads keys from the database into the map cache. Loading is done by merging with
	* existing values, as defined by #updateCache
	* @param keys
	*/
	async loadBatchIntoCache(keys) {
		(await this.db.where({
			primaryKey: this.primaryKey,
			anyOf: keys
		})).filter((row) => keys.includes(row[this.primaryKey])).forEach((row) => this.updateCache(row));
	}
	getFromCache(key) {
		let entry = this.cache.get(key);
		if (!entry) {
			entry = this.newEntry();
			this.cache.set(key, entry);
		}
		return entry;
	}
	/**
	* Saves the values from keys in the map cache to the database. Cached entries are serialised
	* by #serialiseEntry
	* @param keys
	*/
	async saveBatchToDb(keys) {
		const rows = keys.map((key) => {
			const entry = this.getFromCache(key);
			entry.dirty = false;
			return this.serialiseEntry(key, entry);
		});
		await this.db.bulkPut(rows);
	}
	async init() {
		await this.cache.isReady;
		this.batch = [];
		setInterval(() => {
			if (this.batch.length > 0) {
				this.#processBatch([...this.batch]);
				this.batch = [];
			}
		}, this.batchInterval);
	}
	processEntry(entry) {
		this.batch.push(entry);
	}
	async #processBatch(batch) {
		await this.loadBatchIntoCache(batch);
		const today = this.trustedClock.getTimeAsYYYYMMDD();
		const toBeSent = batch.map((token) => [token, this.getFromCache(token)]).filter(([, { lastSent }]) => lastSent !== today);
		const { messages, overflow } = this.createMessagePayloads(toBeSent, this.batchLimit);
		const overflowKeys = new Set(overflow.map((tup) => tup[0]));
		toBeSent.filter((tup) => !overflowKeys.has(tup[0])).forEach(([, _entry]) => {
			const entry = _entry;
			entry.lastSent = this.trustedClock.getTimeAsYYYYMMDD();
		});
		await this.saveBatchToDb(batch);
		for (const message of messages) this.sendMessage(message);
		overflowKeys.forEach((k) => this.processEntry(k));
	}
	/**
	* Periodic task to take unsent values from the database and push them to be sent,
	* as well as cleaning and persisting the map cache.
	*/
	async clean() {
		const batchSize = 1e3;
		const maxSending = Math.ceil(this.options.CLEAN_INTERVAL / this.options.TOKEN_BATCH_INTERVAL * (this.options.TOKEN_BATCH_SIZE * this.options.TOKEN_MESSAGE_SIZE));
		const today = this.trustedClock.getTimeAsYYYYMMDD();
		const now = Date.now();
		const notSentToday = (await this.db.where({ primaryKey: "lastSent" })).filter((token) => token.lastSent !== today).slice(0, batchSize).sort((a, b) => a.created > b.created).filter((row) => row.created < now - this.options.NEW_ENTRY_MIN_AGE);
		const toBeDeleted = [];
		const queuedForSending = [];
		const pruneCutoff = now - this.options.LOW_COUNT_DISCARD_AGE;
		notSentToday.forEach((t) => {
			const hasData = this.hasData(t);
			const minCount = t.count > this.options.MIN_COUNT;
			if (hasData && minCount) queuedForSending.push(t[this.primaryKey]);
			else if (!hasData || t.createdAt < pruneCutoff) toBeDeleted.push(t[this.primaryKey]);
		});
		queuedForSending.splice(maxSending);
		queuedForSending.forEach((v) => this.processEntry(v));
		this.db.bulkDelete(toBeDeleted);
		const saveBatch = [];
		this.cache.forEach((value, key) => {
			if (value.dirty) saveBatch.push(key);
			else if (value.lastSent) this.cache.delete(key);
		});
		await this.saveBatchToDb(saveBatch);
	}
	createMessagePayloads(toBeSent, batchLimit) {
		const overflow = batchLimit ? toBeSent.splice(batchLimit) : [];
		return {
			messages: toBeSent.map(this.createMessagePayload.bind(this)),
			overflow
		};
	}
};
//#endregion
export { CachedEntryPipeline as default };
