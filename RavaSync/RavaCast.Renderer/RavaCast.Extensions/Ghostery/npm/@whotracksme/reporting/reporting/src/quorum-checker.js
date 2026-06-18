import logger_default from "./logger.js";
import SeqExecutor from "./seq-executor.js";
import SelfCheck from "./self-check.js";
import { sha1 } from "./digest.js";
//#region node_modules/@whotracksme/reporting/reporting/src/quorum-checker.js
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
var MINUTE = 60 * 1e3;
var HOUR = 60 * MINUTE;
var QuorumChecker = class {
	constructor({ config, storage, storageKey, bloomFilter, communication }) {
		this.storage = storage;
		this.storageKey = storageKey;
		this.bloomFilter = bloomFilter;
		this.communication = communication;
		if (config.SAFE_QUORUM_CONFIG_ENDPOINT) this.quorumConfigEndpoint = config.SAFE_QUORUM_CONFIG_ENDPOINT;
		else logger_default.warn("Quorum endpoints are not configured.", "Quorum updates will be skipped and all values will be considered to have not reached quorum.");
		this.configTTL = 2 * HOUR;
		this._configUpdateLock = new SeqExecutor();
		this._finishedInitialSyncWithDisk = false;
		this._persistedState = {
			lastUpdated: 0,
			bucket: 0
		};
		this._badKeys = /* @__PURE__ */ new Set();
		this._stats = {
			incQuorum: {
				attempts: 0,
				alreadyVoted: 0,
				success: 0,
				skipped: 0,
				errors: 0
			},
			checkQuorum: {
				attempts: 0,
				success: 0,
				errors: 0,
				results: {
					yes: 0,
					no: 0
				}
			},
			config: {
				attempts: 0,
				success: 0,
				errors: 0
			}
		};
	}
	async sendQuorumIncrement({ text, now = Date.now() } = {}) {
		this._stats.incQuorum.attempts += 1;
		try {
			const quorumKey = `[incQuorum]|${text}`;
			if (this._badKeys.has(text)) {
				try {
					await this.bloomFilter.add(quorumKey);
					logger_default.debug("Successfully cleared one \"badKey\" entry");
					this._badKeys.delete(text);
				} catch (e) {
					logger_default.error("Unable to update bloom filter (when retrying)", e);
				}
				this._stats.incQuorum.alreadyVoted += 1;
				return;
			}
			if (await this.bloomFilter.mightContain(quorumKey)) {
				this._stats.incQuorum.alreadyVoted += 1;
				return;
			}
			await this.updateQuorumConfig();
			if (this._isReadyToSend(now)) {
				const payload = `?hu=${await sha1(text)}&oc=${this._persistedState.bucket}`;
				const response = await this.communication.sendInstant({
					action: "safe-browsing-quorum",
					path: "incrquorum",
					payload,
					method: "GET"
				});
				if (!response.ok) throw new Error(`Failed to increment quorum (${response.statusText})`);
				this._stats.incQuorum.success += 1;
				try {
					await this.bloomFilter.add(quorumKey);
				} catch (e) {
					logger_default.error("Failed to update bloom filters (perhaps the storage is broken?). Might lead to double-voting.", e);
					this._badKeys.add(text);
				}
			} else {
				logger_default.warn("Not contributing to quorum since our config is outdated");
				this._stats.incQuorum.skipped += 1;
			}
		} catch (e) {
			logger_default.error("Failed to increment quorum", e);
			this._stats.incQuorum.errors += 1;
			throw e;
		}
	}
	async checkQuorumConsent({ text }) {
		this._stats.checkQuorum.attempts += 1;
		try {
			if (this._badKeys.has(text)) {
				logger_default.warn("Refusing to check quorum for a key that has been marked as bad");
				this._stats.checkQuorum.errors += 1;
				return false;
			}
			const payload = `?hu=${await sha1(text)}`;
			const response = await this.communication.sendInstant({
				action: "safe-browsing-quorum",
				path: "checkquorum",
				payload,
				method: "GET"
			});
			if (!response.ok) throw new Error(`Failed to get quorum (${response.statusText})`);
			const { result } = await response.json();
			if (result === true) this._stats.checkQuorum.results.yes += 1;
			else if (result === false) this._stats.checkQuorum.results.no += 1;
			else throw new Error("Unexpected result");
			this._stats.checkQuorum.success += 1;
			return result;
		} catch (e) {
			logger_default.error("Failed to check quorum consent", e);
			this._stats.checkQuorum.errors += 1;
			throw e;
		}
	}
	async updateQuorumConfig({ force = false, now = Date.now() } = {}) {
		if (!this.quorumConfigEndpoint) {
			logger_default.info("Quorum server not configured. Update skipped.");
			return;
		}
		await this._configUpdateLock.run(async () => {
			if (!this._finishedInitialSyncWithDisk) {
				try {
					await this._restorePersistedState(now);
				} catch (e) {
					logger_default.warn("Failed to restore the quorum config from disk.", "The configuration will be reloaded from the the server...", e);
				}
				this._finishedInitialSyncWithDisk = true;
			}
			if (!this._isReadyToSend(now) || force) await this._loadFromServer();
		});
	}
	_isReadyToSend(now) {
		return this.quorumConfigEndpoint && this._persistedState.lastUpdated + this.configTTL >= now;
	}
	async _restorePersistedState(now) {
		let persistedState = await this.storage.get(this.storageKey);
		if (!persistedState) {
			logger_default.info("Quorum config config does not exist. This should only happen on the first time the extension is started.");
			return;
		}
		this._ensureValidConfig(persistedState);
		if (persistedState.lastUpdated > now + 5 * MINUTE) {
			logger_default.warn("The timestamps in the quorum config show indications that the system clock was off. Discarding the config to force a resync:", persistedState);
			return;
		}
		this._persistedState = persistedState;
	}
	async _loadFromServer() {
		this._stats.config.attempts += 1;
		const url = this.quorumConfigEndpoint;
		try {
			logger_default.debug("Fetching quorum config from", url);
			const req = await fetch(url);
			if (!req.ok) throw new Error(req.statusText);
			const { oc: bucket } = await req.json();
			const config = {
				lastUpdated: Date.now(),
				bucket
			};
			this._ensureValidConfig(config);
			this._stats.config.success += 1;
			if (bucket !== this._persistedState.bucket) logger_default.debug("Quorum bucket changed from", this._persistedState.bucket, "to", bucket);
			this._persistedState = config;
		} catch (e) {
			logger_default.error(`Failed to update quorum config (from url=${url})`, e);
			this._stats.config.errors += 1;
			throw e;
		}
		try {
			await this.storage.set(this.storageKey, this._persistedState);
		} catch (e) {
			logger_default.warn("Failed to cache quorum configuration (it is safe to continue, but it will be refetched on the next extension start", e);
		}
	}
	_ensureValidConfig(state) {
		if (!state) throw new Error("Missing state");
		const { lastUpdated, bucket } = state;
		if (!Number.isInteger(lastUpdated) || lastUpdated < 0) throw new Error(`Bad timestamp in quorum config (lastUpdated=${lastUpdated})`);
		if (!Number.isInteger(bucket) || bucket < 0 || bucket >= 256) throw new Error(`Bad bucket in quorum config (bucket=${bucket})`);
	}
	async selfChecks(check = new SelfCheck()) {
		if (this._badKeys.size > 0) check.warn("badKeys detected");
		for (const op of [
			"incQuorum",
			"checkQuorum",
			"config"
		]) {
			const { errors, success } = this._stats[op];
			if (errors > 0) {
				if (errors / (errors + success) > .2) check.warn(`Detected high error rates in "${op}" operation`, { [op]: this._stats[op] });
			}
		}
		return check;
	}
};
//#endregion
export { QuorumChecker as default };
