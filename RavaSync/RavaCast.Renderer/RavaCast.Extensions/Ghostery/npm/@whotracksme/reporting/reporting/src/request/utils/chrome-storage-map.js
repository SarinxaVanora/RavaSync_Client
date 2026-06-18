//#region node_modules/@whotracksme/reporting/reporting/src/request/utils/chrome-storage-map.js
/**
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var ChromeStorageMap = class {
	constructor({ sessionApi = typeof chrome !== "undefined" && chrome?.storage?.session, storageKey, softFlushIntervalInMs = 200, hardFlushIntervalInMs = 1e3, ttlInMs = 10080 * 60 * 1e3, maxEntries = 5e3 }) {
		this.sessionApi = sessionApi;
		this._inMemoryMap = /* @__PURE__ */ new Map();
		if (!storageKey) throw new Error("Missing storage key");
		this.storageKey = storageKey;
		this._initialSyncComplete = false;
		this.maxEntries = maxEntries;
		this.ttlInMs = ttlInMs;
		this._ttlMap = /* @__PURE__ */ new Map();
		this.softFlushIntervalInMs = softFlushIntervalInMs;
		this.hardFlushIntervalInMs = hardFlushIntervalInMs;
		this._scheduledFlush = null;
		this._lastFlush = Date.now();
		this._dirty = false;
		this.isReady = new Promise((resolve, reject) => {
			this.sessionApi.get([this.storageKey], (result) => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else {
					const { entries = {}, ttl = {} } = result[this.storageKey] || {};
					this._inMemoryMap = new Map(Object.entries(entries));
					this._ttlMap = new Map(Object.entries(ttl));
					this._initialSyncComplete = true;
					this._expireOldEntries();
					resolve();
				}
			});
		});
	}
	countNonExpiredKeys() {
		this._warnIfOutOfSync();
		this._expireOldEntries();
		return this._inMemoryMap.size;
	}
	get(_key) {
		this._warnIfOutOfSync();
		const key = this.normalizeKey(_key);
		if (this._expireOldEntry(key)) return;
		return this._inMemoryMap.get(key);
	}
	entries() {
		this._warnIfOutOfSync();
		this._expireOldEntries();
		return this._inMemoryMap.entries();
	}
	keys() {
		this._warnIfOutOfSync();
		this._expireOldEntries();
		return this._inMemoryMap.keys();
	}
	values() {
		this._warnIfOutOfSync();
		this._expireOldEntries();
		return this._inMemoryMap.values();
	}
	has(_key) {
		this._warnIfOutOfSync();
		const key = this.normalizeKey(_key);
		if (this._expireOldEntry(key)) return;
		return this._inMemoryMap.has(key);
	}
	forEach(callback) {
		this._warnIfOutOfSync();
		this._expireOldEntries();
		this._inMemoryMap.forEach(callback);
	}
	set(_key, value) {
		this._warnIfOutOfSync();
		if (this._inMemoryMap.size >= this.maxEntries || this._ttlMap.size >= this.maxEntries) {
			console.warn("ChromeStorageMap: Maps are running full (maybe you found a bug?). Purging data to prevent performance impacts.");
			this._inMemoryMap.clear();
			this._ttlMap.clear();
		}
		const key = this.normalizeKey(_key);
		this._inMemoryMap.set(key, value);
		this._ttlMap.set(key, Date.now() + this.ttlInMs);
		this._markAsDirty();
	}
	delete(_key) {
		this._warnIfOutOfSync();
		const key = this.normalizeKey(_key);
		const wasDeleted = this._inMemoryMap.delete(key);
		if (wasDeleted) {
			this._ttlMap.delete(key);
			this._markAsDirty();
		}
		return wasDeleted;
	}
	clear() {
		this._warnIfOutOfSync();
		this._inMemoryMap.clear();
		this._ttlMap.clear();
		this._scheduleAction(new Promise((resolve, reject) => {
			this.sessionApi.remove(this.storageKey, () => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else resolve();
			});
		}));
		this._dirty = false;
	}
	normalizeKey(key) {
		if (typeof key === "number") return key.toString();
		if (typeof key === "string") return key;
		throw new Error(`Unexpected key type (type: ${typeof key}, value: ${key})`);
	}
	_warnIfOutOfSync() {
		if (!this._initialSyncComplete) console.warn(`AutoSyncingMap "${this.storageKey}": out of sync (loading is too slow...)`);
	}
	_expireOldEntries() {
		const now = Date.now();
		let count = 0;
		for (const [key, expireAt] of this._ttlMap.entries()) if (now >= expireAt) {
			this._inMemoryMap.delete(key);
			this._ttlMap.delete(key);
			count += 1;
		}
		if (count > 0) this._markAsDirty();
		return count;
	}
	_expireOldEntry(key) {
		const now = Date.now();
		const expireAt = this._ttlMap.get(key);
		if (expireAt && now >= expireAt) {
			this.delete(key);
			return true;
		}
		return false;
	}
	_markAsDirty() {
		const now = Date.now();
		if (!this._dirty) {
			this._lastFlush = now;
			this._dirty = true;
		}
		const nextForcedFlush = this._lastFlush + this.hardFlushIntervalInMs;
		clearTimeout(this._scheduledFlush);
		if (now >= nextForcedFlush) {
			this._flush();
			this._scheduledFlush = null;
		} else this._scheduledFlush = setTimeout(() => {
			this._flush();
			this._scheduledFlush = null;
		}, Math.min(this.softFlushIntervalInMs, nextForcedFlush - now));
	}
	_flush() {
		if (!this._dirty) return;
		this._scheduleAction(new Promise((resolve, reject) => {
			if (!this._dirty) {
				resolve();
				return;
			}
			this._dirty = false;
			const serialized = {
				entries: Object.fromEntries(this._inMemoryMap),
				ttl: Object.fromEntries(this._ttlMap)
			};
			this.sessionApi.set({ [this.storageKey]: serialized }, () => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else {
					this._lastFlush = Date.now();
					resolve();
				}
			});
		}));
	}
	_scheduleAction(action) {
		const lastSyncPoint = this.isReady;
		this.isReady = lastSyncPoint.then(action).catch(console.error);
		return this.isReady;
	}
};
//#endregion
export { ChromeStorageMap as default };
