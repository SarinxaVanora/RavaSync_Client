import logger_default from "../logger.js";
//#region node_modules/@whotracksme/reporting/reporting/src/request/config.js
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
var RETRY_TIMEOUT = 30 * 1e3;
var VERSION = "0.106";
var COOKIE_MODE = {
	THIRD_PARTY: "thirdparty",
	TRACKERS: "trackers"
};
var DEFAULTS = {
	safekeyValuesThreshold: 4,
	shortTokenLength: 6,
	placeHolder: "ghostery",
	cliqzHeader: "Ghostery-AntiTracking",
	cookieEnabled: true,
	qsEnabled: true,
	sendAntiTrackingHeader: true,
	cookieMode: COOKIE_MODE.THIRD_PARTY
};
/**
* These are attributes which are loaded from the remote CONFIG_URL
* @type {Array}
*/
var REMOTELY_CONFIGURED = ["cookieWhitelist", "compatibilityList"];
var Config = class {
	constructor({ defaults = DEFAULTS, configUrl, remoteWhitelistUrl, localWhitelistUrl }, { db, trustedClock }) {
		this.db = db;
		this.trustedClock = trustedClock;
		this.debugMode = false;
		if (!configUrl) throw new Error("Config requires configUrl");
		this.configUrl = configUrl;
		if (!remoteWhitelistUrl) throw new Error("Config requires remoteWhitelistUrl");
		this.remoteWhitelistUrl = remoteWhitelistUrl;
		if (!localWhitelistUrl) throw new Error("Config requires localWhitelistUrl");
		this.localWhitelistUrl = localWhitelistUrl;
		this.tokenDomainCountThreshold = 2;
		this.safeKeyExpire = 7;
		Object.assign(this, defaults);
	}
	async init() {
		await this._loadConfig();
	}
	unload() {
		clearTimeout(this._retryTimeout);
		this._retryTimeout = null;
	}
	async _loadConfig() {
		await this.db.ready;
		const lastUpdate = await this.db.get("config") || {};
		const day = this.trustedClock.getTimeAsYYYYMMDD();
		if (lastUpdate["config"] && lastUpdate["lastUpdate"] === day) {
			this._updateConfig(lastUpdate["config"]);
			return;
		}
		try {
			const response = await fetch(this.configUrl);
			if (!response.ok) throw new Error(response.text());
			const conf = await response.json();
			this._updateConfig(conf);
			await this.db.set("config", {
				lastUpdate: day,
				config: conf
			});
		} catch (e) {
			logger_default.error("could not load request config", e);
			this._retryTimeout = setTimeout(this._loadConfig.bind(this), RETRY_TIMEOUT);
		}
	}
	_updateConfig(conf) {
		REMOTELY_CONFIGURED.forEach((key) => {
			this[key] = conf[key];
		});
	}
};
//#endregion
export { COOKIE_MODE, VERSION, Config as default };
