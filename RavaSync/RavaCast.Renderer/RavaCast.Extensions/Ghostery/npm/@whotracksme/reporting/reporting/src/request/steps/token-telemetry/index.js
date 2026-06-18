import cachedMD5, { truncatedHash } from "../../../md5.js";
import KeyPipeline, { getSiteTokensMap } from "./key-pipeline.js";
import TokenPipeline from "./token-pipeline.js";
//#region node_modules/@whotracksme/reporting/reporting/src/request/steps/token-telemetry/index.js
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
var DEFAULT_CONFIG = {
	TOKEN_BATCH_INTERVAL: 50 * 1e3,
	TOKEN_BATCH_SIZE: 2,
	TOKEN_MESSAGE_SIZE: 10,
	KEY_BATCH_INTERVAL: 80 * 1e3,
	KEY_BATCH_SIZE: 10,
	KEY_TOKENS_LIMIT: 512,
	CLEAN_INTERVAL: 240 * 1e3,
	TOKEN_BUFFER_TIME: 10 * 1e3,
	NEW_ENTRY_MIN_AGE: 3600 * 1e3,
	MIN_COUNT: 1,
	LOW_COUNT_DISCARD_AGE: 1e3 * 60 * 60 * 24 * 3
};
/**
* Token telemetry: Takes a stream of (tracker, key, value) tuples and generates telemetry in
* the form:
*  - (value, n_sites, n_trackers, safe?), with each value sent max once per calendar day
*  - (key, tracker, site, [values]), with each (key, tracker) tuple sent max once per calendar day
*
* The pipeline is constructed as follows:
*  1. Data comes in from the webrequest-pipeline to #extractKeyTokens
*  2. Tuples are emitted to #subjectTokens.
*  3. #_tokenSubscription subscribes to #subjectTokens, groups and batches it, and stores data
* for each `value` and (tracker, key) tuple in Maps.
*  4. If entries in the Map caches reach a threshold (not sent today and cross site, or older
* than NEW_ENTRY_MIN_AGE), they are pushed to the respective send pipelines for tokens or keys.
*  5. The send pipelines (implemented by CachedEntryPipeline), take a stream of keys from their
* map cache, and check the conditions for sending, given value this entry may have in the
* database. Values which pass this check are pushed to the message sending queue.
*
* The send pipeline also check their cache and database states periodically to trigger data
* persistence, or load old data.
*/
var TokenTelemetry = class {
	constructor(telemetry, qsWhitelist, config, database, shouldCheckToken, options, trustedClock) {
		const opts = {
			...DEFAULT_CONFIG,
			...options
		};
		Object.keys(DEFAULT_CONFIG).forEach((confKey) => {
			this[confKey] = opts[confKey];
		});
		this.telemetry = telemetry;
		this.qsWhitelist = qsWhitelist;
		this.config = config;
		this.trustedClock = trustedClock;
		this.shouldCheckToken = shouldCheckToken;
		this.batch = [];
		this.tokens = new TokenPipeline({
			name: "tokens",
			db: database.tokens,
			trustedClock,
			options: opts,
			sendMessage: (payload) => this.telemetry({
				action: "wtm.attrack.tokensv2",
				payload
			}),
			batchInterval: this.TOKEN_BATCH_INTERVAL,
			batchLimit: this.TOKEN_BATCH_SIZE
		});
		this.keys = new KeyPipeline({
			name: "keys",
			db: database.keys,
			trustedClock,
			options: opts,
			sendMessage: (payload) => this.telemetry({
				action: "wtm.attrack.keysv2",
				payload
			}),
			batchInterval: this.KEY_BATCH_INTERVAL,
			batchLimit: this.KEY_BATCH_SIZE
		});
	}
	async init() {
		await this.tokens.isReady;
		await this.keys.isReady;
		await this.tokens.init();
		await this.keys.init();
		setInterval(() => {
			this.#processBatch([...this.batch]);
			this.batch = [];
		}, this.TOKEN_BUFFER_TIME);
		setInterval(async () => {
			await this.tokens.clean();
			await this.keys.clean();
		}, this.CLEAN_INTERVAL);
	}
	#processBatch(batch) {
		if (batch.length === 0) return;
		const today = this.trustedClock.getTimeAsYYYYMMDD();
		const token = batch[0].token;
		const tokenStats = this.tokens.get(token);
		const entryCutoff = Date.now() - this.NEW_ENTRY_MIN_AGE;
		tokenStats.dirty = true;
		batch.forEach((entry) => {
			if (!tokenStats.sites.includes(entry.fp)) tokenStats.sites.push(entry.fp);
			if (!tokenStats.trackers.includes(entry.tp)) tokenStats.trackers.push(entry.tp);
			tokenStats.safe = tokenStats.safe && entry.safe;
			const keyKey = `${entry.tp}:${entry.key}`;
			const keyStats = this.keys.get(keyKey);
			keyStats.key = entry.key;
			keyStats.tracker = entry.tp;
			keyStats.dirty = true;
			const siteTokens = getSiteTokensMap(keyStats.sitesTokens, entry.fp);
			siteTokens[entry.token] = entry.safe;
			if (keyStats.lastSent !== today && (Object.keys(keyStats.sitesTokens).length > 1 || keyStats.count > this.MIN_COUNT && keyStats.created < entryCutoff)) this.keys.processEntry(keyKey);
		});
		if (tokenStats.lastSent !== today && (tokenStats.sites.length > 1 || tokenStats.count > this.MIN_COUNT && tokenStats.created < entryCutoff)) this.tokens.processEntry(token);
	}
	extractKeyTokens(state) {
		if (state.isPrivate) return true;
		const keyTokens = state.urlParts.extractKeyValues().params;
		if (keyTokens.length > 0) {
			const firstParty = truncatedHash(state.tabUrlParts.generalDomain);
			const generalDomain = truncatedHash(state.urlParts.generalDomain);
			const isTracker = this.qsWhitelist.isTrackerDomain(generalDomain);
			keyTokens.forEach(([k, v]) => {
				if (!this.shouldCheckToken(v)) return;
				const token = cachedMD5(v);
				const key = cachedMD5(k);
				const safe = !isTracker || this.qsWhitelist.isSafeKey(generalDomain, key) || this.qsWhitelist.isSafeToken(generalDomain, token);
				this.batch.push({
					day: this.trustedClock.getTimeAsYYYYMMDD(),
					key,
					token,
					tp: generalDomain,
					fp: firstParty,
					safe,
					isTracker
				});
			});
		}
		return true;
	}
};
//#endregion
export { TokenTelemetry as default };
