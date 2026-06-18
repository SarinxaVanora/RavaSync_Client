import Bowser from "../../../../bowser/src/bowser.js";
import logger_default from "./logger.js";
import { lazyInitAsync, requireParam, requireString } from "./utils.js";
//#region node_modules/@whotracksme/reporting/reporting/src/alive-message-generator.js
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
* Responsible for generating information to be included in the "alive" message.
* The purpose of the additional information in the alive message is to detect
* unusual traffic, for instance, if a new browser version introduced a regression.
*
* The information will only be included if it has been seen by multiple users.
* The data itself should not be sensitive, still there is no point in including it
* as only "big hitters" are interesting. For instance, should there be a problem in
* an obscure setup, it is less interesting then if a recent Firefox or Chrome release
* breaks something on Windows.
*/
var AliveMessageGenerator = class {
	constructor({ navigatorApi, quorumChecker, storage, storageKey }) {
		this.quorumChecker = requireParam(quorumChecker);
		this.storage = requireParam(storage);
		this.storageKey = requireString(storageKey);
		this.navigatorApi = navigatorApi || globalThis.navigator;
		this.staticConfigProvider = lazyInitAsync(this._generateBaseConfig.bind(this));
	}
	async generateMessage(ctry, hour) {
		const config = {
			...await this.staticConfigProvider(),
			ctry
		};
		try {
			if (await this._reachedQuorum(config)) {
				config.t = hour;
				logger_default.debug("Found the following configuration safe to share:", config);
				return config;
			}
			logger_default.debug("Omitting the config, as it is not popular enough:", config);
		} catch (e) {
			logger_default.warn("Failed to confirm that the configuration is popular enough:", config, e);
		}
		return {
			browser: "",
			version: "",
			os: "",
			platform: "",
			engine: "",
			language: "",
			ctry: "--",
			t: hour
		};
	}
	async _generateBaseConfig() {
		let browser, version, os, platform, engine;
		const { language, userAgent } = this.navigatorApi;
		if (userAgent) {
			const browserInfo = Bowser.parse(userAgent);
			browser = browserInfo.browser?.name;
			if (browser === "Chrome" && await this._looksLikeBrave()) {
				logger_default.debug("Chrome user agent detected, but the API says it is Brave");
				browser = "Chrome (Brave)";
			}
			os = browserInfo.os?.name;
			platform = browserInfo.platform?.type;
			engine = browserInfo.engine?.name;
			const majorVersion = parseInt(browserInfo.browser?.version, 10);
			if (!isNaN(majorVersion)) version = String(majorVersion);
		}
		return {
			browser: browser || "",
			version: version || "",
			os: os || "",
			platform: platform || "",
			engine: engine || "",
			language: language || ""
		};
	}
	/**
	* Checks that the additional configuration is popular enough to be shared.
	* As long as the configuration does not change, keep the initial result of the
	* quorum check. Otherwise, it might happen that it reaches quorum over time
	* by itself (provided that we keep sharing it and it never changes).
	*
	* If there is a false positive (a popular config being not detected), it will
	* eventually recover as the configuration will changed over time (e.g. after each
	* browser update).
	*/
	async _reachedQuorum(config) {
		const newConfig = this._deterministicStringify(config);
		let reachedQuorum = null;
		try {
			const oldValue = await this.storage.get(this.storageKey);
			if (oldValue && newConfig === oldValue.config) reachedQuorum = oldValue.reachedQuorum;
		} catch (e) {
			logger_default.warn("Ignore errors when reading the old state, but check the quorum again", e);
		}
		if (reachedQuorum === null) {
			await this.quorumChecker.sendQuorumIncrement({ text: newConfig });
			reachedQuorum = await this.quorumChecker.checkQuorumConsent({ text: newConfig });
			await this.storage.set(this.storageKey, {
				config: newConfig,
				reachedQuorum
			});
		}
		return reachedQuorum;
	}
	_deterministicStringify(obj) {
		return JSON.stringify(Object.fromEntries(Object.entries(obj).sort()));
	}
	async _looksLikeBrave() {
		try {
			return !!(this.navigatorApi.brave && await this.navigatorApi.brave.isBrave());
		} catch (e) {
			return false;
		}
	}
};
//#endregion
export { AliveMessageGenerator as default };
