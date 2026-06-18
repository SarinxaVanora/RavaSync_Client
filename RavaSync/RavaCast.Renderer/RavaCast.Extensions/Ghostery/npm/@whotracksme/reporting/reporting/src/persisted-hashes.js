import logger_default from "./logger.js";
//#region node_modules/@whotracksme/reporting/reporting/src/persisted-hashes.js
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
/**
* From time to time, we need to iterate through all keys
* to actively remove expired ones.
*/
var DEFAULT_PRUNE_INTERVAL = 6 * (60 * (60 * 1e3));
/**
* This should be so high that under normal operations
* it must be impossible to reach.
*/
var THRESHOLD_TO_RESET_ALL_KEYS = 5e4;
function isExpired(ts) {
	return Date.now() >= ts;
}
var PersistedHashes = class {
	constructor({ storage, storageKey }) {
		this.storage = storage;
		this.storageKey = storageKey;
		this.entries = /* @__PURE__ */ new Map();
		this.nextPrune = 0;
		this._pending = null;
		this._dirty = false;
	}
	async _ready() {
		if (!this._pending) this._pending = this._loadFromDisk().catch((e) => {
			logger_default.info("Could not restore previous state. It is normal to see this message on a new profile.", e);
		});
		await this._pending;
		this._expireOldEntries();
	}
	async has(key) {
		await this._ready();
		const value = this.entries.get(key);
		if (!value) return false;
		if (isExpired(value)) {
			this.entries.delete(key);
			this._dirty = true;
			return false;
		}
		return true;
	}
	async add(key, expireAt) {
		if (!Number.isInteger(expireAt)) throw new Error(`Bad expiredAt timestamp: ${expireAt}`);
		await this._ready();
		const oldMapping = this.entries.get(key);
		if (oldMapping && !isExpired(oldMapping)) return false;
		if (this.entries.size >= THRESHOLD_TO_RESET_ALL_KEYS) {
			logger_default.error("The hashes on the profile ran full. Purging now to prevent performance impacts.");
			this.entries.clear();
		}
		this.entries.set(key, expireAt);
		this._dirty = true;
		setTimeout(() => {
			this.flush();
		}, 0);
		return true;
	}
	async delete(key) {
		await this._ready();
		if (this.entries.delete(key)) {
			this._dirty = true;
			return true;
		}
		return false;
	}
	async clear() {
		await this._ready();
		if (this.entries.size > 0) {
			this.entries.clear();
			this._dirty = true;
		}
	}
	async flush() {
		await this._ready();
		this._pending = this._pending.then(async () => {
			if (this._dirty) await this._persist();
		}).catch(() => {});
		await this._pending;
	}
	async _loadFromDisk() {
		const values = await this.storage.get(this.storageKey);
		if (values) {
			const { nextPrune, hashes } = values;
			if (!Number.isInteger(nextPrune)) throw new Error(`Corrupted nextPrune: ${nextPrune}`);
			if (!Array.isArray(hashes)) throw new Error(`Corrupted hashes: ${hashes}`);
			this.entries = new Map(hashes);
			this.nextPrune = nextPrune;
			this._dirty = false;
		}
	}
	async _persist() {
		this._dirty = false;
		await this.storage.set(this.storageKey, {
			hashes: [...this.entries],
			nextPrune: this.nextPrune,
			version: 1
		});
	}
	_expireOldEntries() {
		const now = Date.now();
		if (now >= this.nextPrune) {
			const oldSize = this.entries.size;
			this.entries = new Map([...this.entries].filter(([, expireAt]) => expireAt > now));
			logger_default.info("cleanup:", oldSize - this.entries.size, "hashes have expired");
			this.nextPrune = now + DEFAULT_PRUNE_INTERVAL;
			this._dirty = true;
			setTimeout(() => {
				this.flush();
			}, 0);
		}
	}
};
//#endregion
export { PersistedHashes as default };
