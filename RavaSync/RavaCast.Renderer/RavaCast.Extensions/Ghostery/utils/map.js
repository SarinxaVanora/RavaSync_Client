//#region src/utils/map.js
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
var storage = chrome.storage.session || chrome.storage.local;
var AutoSyncingMap = class {
	static async get(storageKey, key) {
		return (await storage.get([storageKey]))[storageKey]?.entries[key];
	}
	constructor({ storageKey, softFlushIntervalInMs = 200, hardFlushIntervalInMs = 1e3, ttlInMs = 1440 * 60 * 1e3, maxEntries = 1e3 }) {
		if (!storageKey) throw new Error("Missing storage key");
		this.storageKey = storageKey;
		this.inMemoryMap = /* @__PURE__ */ new Map();
		this._initialSyncComplete = false;
		this.maxEntries = maxEntries;
		this.ttlInMs = ttlInMs;
		this._ttlMap = /* @__PURE__ */ new Map();
		this.softFlushIntervalInMs = softFlushIntervalInMs;
		this.hardFlushIntervalInMs = hardFlushIntervalInMs;
		this._scheduledFlush = null;
		this._lastFlush = Date.now();
		this._dirty = false;
		this._pending = new Promise((resolve, reject) => {
			storage.get([this.storageKey], (result) => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else {
					const { entries = {}, ttl = {} } = result[this.storageKey] || {};
					this.inMemoryMap = new Map(Object.entries(entries));
					this._ttlMap = new Map(Object.entries(ttl));
					this._initialSyncComplete = true;
					this.expireOldEntries();
					resolve();
				}
			});
		});
	}
	_warnIfOutOfSync() {
		if (!this._initialSyncComplete) console.warn("AutoSyncingMap: out of sync (loading is too slow...)");
	}
	get(_key) {
		this._warnIfOutOfSync();
		const key = this._normalizeKey(_key);
		return this.inMemoryMap.get(key);
	}
	set(_key, value) {
		this._warnIfOutOfSync();
		if (this.inMemoryMap.size >= this.maxEntries || this._ttlMap.size >= this.maxEntries) {
			console.warn("AutoSyncingMap: Maps are running full (maybe you found a bug?). Purging data to prevent performance impacts.");
			this.inMemoryMap.clear();
			this._ttlMap.clear();
		}
		const key = this._normalizeKey(_key);
		this.inMemoryMap.set(key, value);
		this._ttlMap.set(key, Date.now() + this.ttlInMs);
		this._markAsDirty();
	}
	delete(_key) {
		this._warnIfOutOfSync();
		const key = this._normalizeKey(_key);
		const wasDeleted = this.inMemoryMap.delete(key);
		if (wasDeleted) {
			this._ttlMap.delete(key);
			this._markAsDirty();
		}
		return wasDeleted;
	}
	clear() {
		this._warnIfOutOfSync();
		this.inMemoryMap.clear();
		this._ttlMap.clear();
		this._scheduleAction(new Promise((resolve, reject) => {
			storage.remove(this.storageKey, () => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else resolve();
			});
		}));
		this._dirty = false;
	}
	expireOldEntries() {
		const now = Date.now();
		let count = 0;
		for (const [key, expireAt] of this._ttlMap.entries()) if (now >= expireAt) {
			this.inMemoryMap.delete(key);
			this._ttlMap.delete(key);
			count += 1;
		}
		if (count > 0) this._markAsDirty();
		return count;
	}
	_normalizeKey(key) {
		if (typeof key === "number") return key.toString();
		if (typeof key === "string") return key;
		throw new Error(`Unexpected key type (type: ${typeof key}, value: ${key})`);
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
				entries: Object.fromEntries(this.inMemoryMap),
				ttl: Object.fromEntries(this._ttlMap)
			};
			storage.set({ [this.storageKey]: serialized }, () => {
				if (chrome.runtime.lastError) reject(chrome.runtime.lastError);
				else {
					this._lastFlush = Date.now();
					resolve();
				}
			});
		}));
	}
	_scheduleAction(action) {
		const lastSyncPoint = this._pending;
		this._pending = lastSyncPoint.then(action).catch(console.error);
		return this._pending;
	}
};
//#endregion
export { AutoSyncingMap as default };
