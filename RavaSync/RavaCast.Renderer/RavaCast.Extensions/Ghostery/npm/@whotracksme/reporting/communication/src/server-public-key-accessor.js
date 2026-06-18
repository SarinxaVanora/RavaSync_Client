import logger_default from "./logger.js";
import { getTimeAsYYYYMMDD } from "./timestamps.js";
import { fromBase64 } from "./encoding.js";
import { FailedToFetchPublicKeys } from "./errors.js";
//#region node_modules/@whotracksme/reporting/communication/src/server-public-key-accessor.js
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
function isYYYYMMDD(date) {
	return typeof date === "string" && /^[0-9]{8}$/.test(date);
}
var ServerPublicKeyAccessor = class {
	constructor({ config, database }) {
		this.collectorUrl = config.COLLECTOR_DIRECT_URL;
		this.database = database;
		this.storageKey = "server-ecdh-keys";
		this._knownKeys = /* @__PURE__ */ new Map();
	}
	async getKey(today = getTimeAsYYYYMMDD()) {
		if (!this._knownKeys.get(today)) {
			if (!this._pending) {
				this._pending = this._updateCache(today);
				this._pending.catch(() => {}).then(() => {
					this._pending = null;
				});
			}
			await this._pending;
		}
		const key = this._knownKeys.get(today);
		if (key) return {
			date: today,
			publicKey: key.imported
		};
		throw new Error(`No server's public key was found for today=${today}`);
	}
	async _updateCache(today) {
		let knownKeys;
		try {
			const keysFromDisk = await this.database.get(this.storageKey).catch(() => null);
			if (keysFromDisk && keysFromDisk.some(([date]) => date === today)) {
				logger_default.debug("Server keys on disk are still valid");
				knownKeys = await this.importAndVerifyPubKeys(keysFromDisk);
			} else logger_default.info("Server keys on disk need to be refetched. Expected:", today);
		} catch (e) {
			logger_default.warn(`Ignoring corrupted server keys (storageKey: ${this.storageKey}). Reload from network.`, e);
		}
		if (!knownKeys) {
			const pubKeys = await this._fetchPublicKeys();
			const allKeys = Object.keys(pubKeys).filter(isYYYYMMDD).map((date) => [date, fromBase64(pubKeys[date])]);
			knownKeys = await this.importAndVerifyPubKeys(allKeys);
			try {
				const entry = [...knownKeys].map(([date, { key }]) => [date, key]);
				await this.database.set(this.storageKey, entry);
			} catch (e) {
				logger_default.warn("Failed to cache server keys to disk.", e);
			}
		}
		this._knownKeys = knownKeys;
	}
	async _fetchPublicKeys() {
		const url = `${this.collectorUrl}/config?fields=pubKeys`;
		logger_default.info("Fetching new server public keys from", url);
		try {
			const response = await fetch(url, {
				method: "GET",
				credentials: "omit",
				redirect: "manual"
			});
			if (!response.ok) throw new Error(`Failed to get config (${response.statusText}) from '${url}'`);
			const { pubKeys } = await response.json();
			logger_default.info("Fetched server public keys:", pubKeys);
			return pubKeys;
		} catch (e) {
			throw new FailedToFetchPublicKeys(`Failed to fetch public keys from '${url}'`, { cause: e });
		}
	}
	async importAndVerifyPubKeys(allKeys) {
		return new Map(await Promise.all(allKeys.map(async ([date, key]) => {
			return [date, {
				key,
				imported: await crypto.subtle.importKey("raw", key, {
					name: "ECDH",
					namedCurve: "P-256"
				}, false, [])
			}];
		})));
	}
};
//#endregion
export { ServerPublicKeyAccessor as default };
