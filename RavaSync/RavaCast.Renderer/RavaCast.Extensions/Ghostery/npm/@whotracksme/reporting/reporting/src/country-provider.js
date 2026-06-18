import logger_default from "./logger.js";
import { clamp, randBetween } from "./utils.js";
//#region node_modules/@whotracksme/reporting/reporting/src/country-provider.js
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
var SECOND = 1e3;
var MINUTE = 60 * SECOND;
var HOUR = 60 * MINUTE;
var DAY = 24 * HOUR;
/**
* If you need to introduce incompatible changes to the the state
* persistence, you can bump this number to clear the persisted cache.
* It will be as if you start with a fresh installation of the extension.
*
* (If you are not sure whether the change is incompatible, it is
* a good idea to be conservative and bump this number anyway; it will
* only introduce little overhead.)
*/
var DB_VERSION = 1;
/**
* Responsible for fetching the country information from the config API endpoint
* and keeping it up-to-date. Although you can force updates through the "update",
* it should not be needed to explicitly call it.
*
* WARNING: When using this class, only access data through the "getSafeCountryCode"
* function. Why? Because otherwise, it is easy to accidentially allow fingerprinting
* (see the comments in "_sanitizeCountry" for details).
*/
var CountryProvider = class {
	constructor({ config, storage, storageKey }) {
		this.allowedCountryCodes = config.ALLOWED_COUNTRY_CODES;
		if (!this.allowedCountryCodes) throw new Error("config.ALLOWED_COUNTRY_CODES not set");
		this.configApiEndpoint = config.CONFIG_URL;
		if (!this.configApiEndpoint) throw new Error("config.CONFIG_URL not set");
		this.storage = storage;
		this.storageKey = storageKey;
		this.ctryInfo = null;
		this.cacheExpiration = {
			min: 22 * HOUR,
			max: 26 * HOUR
		};
	}
	async init({ now = Date.now() } = {}) {
		if (this.ctryInfo) {
			logger_default.debug("CountryProvider is already initialized");
			return;
		}
		try {
			const cachedEntry = await this.storage.get(this.storageKey);
			if (cachedEntry) {
				this.ctryInfo = this._validateCtryInfo(cachedEntry, now);
				this._nonBlockingUpdateInBackground(now);
				return;
			}
		} catch (e) {
			logger_default.warn("Failed to get a value from cache", e);
		}
		logger_default.info("Country information not cached. This should only happen on the first time the extension is started.");
		this.ctryInfo = {
			dbVersion: DB_VERSION,
			unsafeCtryFromApi: "--",
			safeCtry: "--",
			lastSuccessAt: 0,
			lastAttemptAt: 0,
			skipAttemptsUntil: 0,
			failedAttemptsInARows: 0
		};
		try {
			await this.update({ now });
		} catch (e) {
			logger_default.warn("Unable to get country information. Falling back to \"--\"", e);
		}
	}
	getSafeCountryCode({ skipUpdate = false, now = Date.now() } = {}) {
		if (!this.ctryInfo) throw new Error("Illegal state: forgot to call init()");
		if (!skipUpdate) this._nonBlockingUpdateInBackground(now);
		return this.ctryInfo.safeCtry;
	}
	/**
	* Normally, it should not be needed to call this function directly.
	*
	* It is safe though to call it in a loop if needed. Multiple calls
	* will not lead to multiple HTTP requests and the performance impact
	* should not be noticeable in most scenarios.
	*/
	async update({ force = false, now = Date.now() } = {}) {
		if (this._pendingUpdate) {
			logger_default.debug("update already in progress...");
			await this._pendingUpdate;
			return;
		}
		if (!force && now < this.ctryInfo.skipAttemptsUntil) return;
		this._pendingUpdate = (async () => {
			try {
				const url = this.configApiEndpoint;
				try {
					const response = await fetch(url, {
						method: "GET",
						cache: "no-cache",
						credentials: "omit"
					});
					if (!response.ok) throw new Error(`Failed to reach ${url}: ${response.statusText}`);
					const { location } = await response.json();
					const cooldown = randBetween(this.cacheExpiration.min, this.cacheExpiration.max);
					this.ctryInfo = this._validateCtryInfo({
						dbVersion: DB_VERSION,
						unsafeCtryFromApi: location,
						safeCtry: this._sanitizeCountry(location),
						lastSuccessAt: now,
						lastAttemptAt: now,
						skipAttemptsUntil: now + cooldown,
						failedAttemptsInARows: 0
					}, now);
					logger_default.debug("Updated country information:", this.ctryInfo);
				} catch (e) {
					const failedAttemptsInARows = this.ctryInfo.failedAttemptsInARows + 1;
					const avgCooldown = failedAttemptsInARows * (30 * SECOND);
					const cooldown = clamp({
						value: randBetween(avgCooldown / 1.5, 1.5 * avgCooldown),
						min: 3 * SECOND,
						max: 3 * DAY
					});
					this.ctryInfo = {
						...this.ctryInfo,
						failedAttemptsInARows,
						skipAttemptsUntil: now + cooldown
					};
					logger_default.warn("Failed to update country. Cooldown until:", new Date(this.ctryInfo.skipAttemptsUntil), e);
					throw e;
				}
			} finally {
				await this._syncCacheToDisk();
				this._pendingUpdate = null;
			}
		})();
		await this._pendingUpdate;
	}
	_nonBlockingUpdateInBackground(now = Date.now()) {
		this.update({ now }).catch((e) => {
			logger_default.debug(`Unable to update country (${e}). Continuing with existing settings:`, this.ctryInfo);
		});
	}
	async _syncCacheToDisk() {
		try {
			await this.storage.set(this.storageKey, this.ctryInfo);
		} catch (e) {
			logger_default.error("Unable to update cache:", this.storageKey, e);
		}
	}
	/**
	* As long as there are enough other users, revealing the country
	* will not compromise anonymity. Only if the user base is too low
	* (e.g., Liechtenstein), we have to be careful. In that case,
	* do not reveal the country, otherwise fingerprinting attacks
	* could be possible.
	*
	* The server side will already hide countries with few users.
	* But as the expected number of users varies between products,
	* the client can additionally filter the list further (the
	* information for it is provided by the config).
	*
	* Including the unmodified country (without sanitizing) in messages,
	* is also not recommended, since it would allow fingerprinting attacks
	* should an attacker get control of the server: The attacker could modify
	* the API to return unique identifiers instead of the country. If the
	* client would include these ID in messages, the server could then link
	* messages from the same user.
	*/
	_sanitizeCountry(ctry) {
		return this.allowedCountryCodes.includes(ctry) ? ctry : "--";
	}
	/**
	* Runs some sanity checks on the data. You can use it to reject corrupted
	* data that has been fetch from the API, or has been loaded from the storage.
	*
	* If this function throws, the new value will be dropped. If available,
	* the client will continue to the last good value; otherwise, it will
	* fallback to "--".
	*/
	_validateCtryInfo(ctryInfo, now) {
		const { dbVersion, unsafeCtryFromApi, safeCtry, lastSuccessAt, lastAttemptAt, skipAttemptsUntil } = ctryInfo;
		if (dbVersion !== DB_VERSION) throw new Error(`Data schema changed (expected: ${DB_VERSION}, but got ${dbVersion})`);
		const checkCtry = (ctry) => {
			if (typeof ctry !== "string" || !ctry || ctry.length > 4) {
				const msg = `Rejecting data with corrupted country: ${ctry}`;
				logger_default.warn(msg, ctryInfo);
				throw new Error(msg);
			}
		};
		checkCtry(unsafeCtryFromApi);
		checkCtry(safeCtry);
		if (lastSuccessAt > now + 5 * MINUTE || lastAttemptAt > now + 5 * MINUTE || skipAttemptsUntil > now + 90 * DAY) {
			const msg = "Rejecting data with corrupted timestamps";
			logger_default.warn(msg, ctryInfo);
			throw new Error(msg);
		}
		return ctryInfo;
	}
};
//#endregion
export { CountryProvider as default };
