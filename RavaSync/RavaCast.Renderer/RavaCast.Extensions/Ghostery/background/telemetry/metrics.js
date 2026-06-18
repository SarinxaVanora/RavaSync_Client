import getBrowserInfo from "../../utils/browser-info.js";
import "../../npm/@ghostery/config/dist/esm/flags.js";
import getDefaultLanguage from "./language.js";
//#region src/background/telemetry/metrics.js
/**
* Metrics
*
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017 Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var ACTIVE_PING_DELAY = 600 * 1e3;
/**
* Allows to run async operations one by one (FIFO, first-in first-out).
* The execution of function will only be started once all previously
* scheduled functions have resolved (either successfully or by an exception).
*
* (copied from src/seq-executor.js)
*/
var SeqExecutor = class {
	constructor() {
		this.pending = Promise.resolve();
	}
	async run(func) {
		let result;
		let failed = false;
		this.pending = this.pending.then(async () => {
			try {
				result = await func();
			} catch (e) {
				failed = true;
				result = e;
			}
		});
		await this.pending;
		if (failed) throw result;
		return result;
	}
	async waitForAll() {
		await this.pending;
	}
};
/**
* Helper for building query string key value pairs
*
* @since 8.5.4
* @param  {string}  query		param to be included in string
* @param  {string}  value		number value to be passed on through qeury string
* @param  {boolean} queryStart	indicates whether the returned string is intended for start of a query
* @return {string}         	complete query component
*/
var buildQueryPair = (query, value, queryStart = false) => `${queryStart ? "?" : "&"}${query}=${encodeURIComponent(value)}`;
/**
* Process URLs and returns the query string as an object.
* @memberOf BackgroundUtils
* @param  {string} src 	the source url
* @return {Object}			contains parts of parsed query as its properties
*/
function processUrlQuery(src) {
	if (!src) return {};
	try {
		const res = {};
		for (const [key, value] of new URL(src).searchParams.entries()) res[key] = value;
		return res;
	} catch {
		return {};
	}
}
var FREQUENCIES = {
	daily: 864e5,
	weekly: 6048e5,
	monthly: 24192e5
};
var CAMPAIGN_METRICS = [
	"install",
	"active",
	"uninstall"
];
/**
* Class for handling telemetry pings.
* @memberOf  BackgroundClasses
*/
var Metrics = class {
	constructor({ getConf, log, EXTENSION_VERSION, METRICS_BASE_URL, saveStorage, storage }) {
		this.EXTENSION_VERSION = EXTENSION_VERSION;
		this.METRICS_BASE_URL = METRICS_BASE_URL;
		this.getConf = getConf;
		this.log = log;
		this.saveStorage = saveStorage;
		this.storage = storage || {};
		this._seqExecutor = new SeqExecutor();
	}
	/**
	* Check if the extension was just installed
	* @returns {boolean} true if the extension was just installed
	*/
	isJustInstalled() {
		return !this.storage.install_all;
	}
	async setUTMs({ utm_source = "", utm_campaign = "" }) {
		this.storage.utm_source = utm_source;
		this.storage.utm_campaign = utm_campaign;
		await this.saveStorage(this.storage);
	}
	/**
	* Prepare data and send telemetry pings.
	* @param {string} type    type of the telemetry ping
	*/
	ping(type) {
		return this._seqExecutor.run(async () => {
			switch (type) {
				case "install":
					await this._recordInstall();
					break;
				case "active":
					await this._recordActive();
					break;
				case "engaged":
					await this._recordEngaged();
					break;
				case "install_complete":
					await this._recordInstallComplete();
					break;
				default:
					this.log(`metrics ping() error: ping name ${type} not found`);
					break;
			}
		});
	}
	/**
	* Set uninstall url
	*/
	async setUninstallUrl() {
		const url = await this._buildMetricsUrl("uninstall");
		chrome.runtime.setUninstallURL(url);
	}
	/**
	* Build telemetry URL
	*
	* @private
	*
	* @param  {string} type     	ping type
	* @param  {string} frequency 	ping frequency
	* @return {string}          	complete telemetry url
	*/
	async _buildMetricsUrl(type, frequency) {
		const browserInfo = await getBrowserInfo();
		const conf = await this.getConf();
		const frequencyString = type !== "uninstall" ? `/${frequency}` : "";
		let metrics_url = `${this.METRICS_BASE_URL}/${type}${frequencyString}?gr=-1`;
		metrics_url += buildQueryPair("v", this.EXTENSION_VERSION) + buildQueryPair("ua", browserInfo.token) + buildQueryPair("os", browserInfo.os) + buildQueryPair("l", getDefaultLanguage()) + buildQueryPair("bv", browserInfo.version) + buildQueryPair("id", this.storage.installDate) + buildQueryPair("tp", Number(conf.userSettings?.isOnToolbar ?? -1)) + buildQueryPair("zap", !conf.config.hasFlag("modes") ? "-1" : this.storage.modeTouched ? conf.options.mode === "zap" ? "3" : "2" : conf.options.mode === "zap" ? "1" : "0") + buildQueryPair("aia", conf.isAllowedIncognitoAccess ? "1" : "0") + buildQueryPair("oc", this.storage.install_complete_all ? "1" : "0") + buildQueryPair("pv", conf.yesterdayPages >= 10 ? "2" : conf.yesterdayPages > 0 ? "1" : "0") + buildQueryPair("se", this._getSearchSignal());
		if (type !== "uninstall") metrics_url += buildQueryPair("ab", conf.options.blockAds ? "1" : "0") + buildQueryPair("sm", conf.options.blockAnnoyances ? "1" : "0") + buildQueryPair("at", conf.options.blockTrackers ? "1" : "0") + buildQueryPair("rc", this._getRecencyActive(type, frequency).toString()) + buildQueryPair("va", this._getVelocityActive(type).toString()) + buildQueryPair("re", this._getRecencyEngaged(type, frequency).toString()) + buildQueryPair("ve", this._getVelocityEngaged(type).toString()) + buildQueryPair("hw", conf.options.feedback ? "1" : "0");
		if (CAMPAIGN_METRICS.includes(type)) metrics_url += buildQueryPair("us", this.storage.utm_source) + buildQueryPair("uc", this.storage.utm_campaign);
		return metrics_url;
	}
	/**
	* Send Ping Request
	*
	* @private
	*
	* @param {string} 		type 				ping type
	* @param {array} 		[frequencies = ['all']] 	array of ping frequencies
	*/
	async _sendReq(type, frequencies = ["all"]) {
		const now = Date.now();
		let storageDirty = false;
		for (const frequency of frequencies) {
			const key = `${type}_${frequency}`;
			if (!this.storage[key] && type !== "engaged" && type !== "active" && frequency !== "all") {
				this.log(`ping: initializing metrics (type=${type}, frequency=${frequency}) [should be seen only once per type and frequency]`);
				this.storage[key] = now;
				storageDirty = true;
			}
			if (this.storage[key] > now + 1e3 * 60 * 60 * 24 * 365) {
				this.log(`ping: resetting metrics (type=${type}, frequency=${frequency}) [clock jump detected]`);
				this.storage[key] = now;
				storageDirty = true;
			}
		}
		if (storageDirty) await this.saveStorage(this.storage);
		const preparedRequests = [];
		for (const frequency of frequencies) {
			const key = `${type}_${frequency}`;
			if (frequency === "all" || now >= (this.storage[key] || 0) + FREQUENCIES[frequency]) {
				this.storage[key] = now;
				this.log(`ping: ${frequency} ${type} (preparing...)`);
				preparedRequests.push({
					type,
					frequency
				});
			}
		}
		if (preparedRequests.length === 0) return;
		try {
			await this.saveStorage(this.storage);
		} catch (err) {
			throw new Error(`Error sending metrics (type=${type}. Failed to write on disk.`, { cause: err });
		}
		const headers = new Headers();
		headers.append("Content-Type", "image/gif");
		const options = {
			headers,
			referrerPolicy: "no-referrer",
			credentials: "omit",
			type: "image"
		};
		const results = await Promise.allSettled(preparedRequests.map(async ({ type, frequency }) => {
			const metrics_url = await this._buildMetricsUrl(type, frequency);
			const request = new Request(metrics_url, options);
			const response = await fetch(request);
			if (!(response.status >= 200 && response.status < 400)) throw new Error(`Error sending metrics (type=${type}, status=${response.status}`);
			this.log(`ping: ${frequency} ${type} (successfully sent)`);
		}));
		for (const { status, reason } of results) if (status === "rejected") throw reason;
		this.log(`ping: sending metrics of type=${type} succeeded.`);
	}
	/**
	* Calculate days since the last daily active ping.
	*
	* @private
	*
	* @return {number} in days since the last daily active ping
	*/
	_getRecencyActive(type, frequency) {
		if (this.storage.active_daily && (type === "active" || type === "engaged") && frequency === "daily") return Math.floor((Date.now() - this.storage.active_daily) / 864e5);
		return -1;
	}
	/**
	* Calculate days since the last daily engaged ping.
	*
	* @private
	*
	* @return {number}	in days since the last daily engaged ping
	*/
	_getRecencyEngaged(type, frequency) {
		if (this.storage.engaged_daily && (type === "active" || type === "engaged") && frequency === "daily") return Math.floor((Date.now() - this.storage.engaged_daily) / 864e5);
		return -1;
	}
	/**
	* Get the Active Velocity
	* @private
	* @return {number}  The Active Velocity
	*/
	_getVelocityActive(type) {
		if (type !== "active" && type !== "engaged") return -1;
		const active_daily_velocity = this.storage.active_daily_velocity || [];
		const today = Math.floor(Date.now() / 864e5);
		return active_daily_velocity.filter((el) => el > today - 7).length;
	}
	/**
	* Get the Engaged Velocity
	* @private
	* @return {number}  The Engaged Velocity
	*/
	_getVelocityEngaged(type) {
		if (type !== "active" && type !== "engaged") return -1;
		const engaged_daily_velocity = this.storage.engaged_daily_velocity || [];
		const today = Math.floor(Date.now() / 864e5);
		return engaged_daily_velocity.filter((el) => el > today - 7).length;
	}
	/**
	* Get the SERP visit signal for the previous full UTC day.
	* Using yesterday avoids bias from the time-of-day at which the
	* ping fires, since today's counter is mid-aggregation.
	* @private
	* @return {string} '0' = no visits, '1' = less than 10, '2' = 10 or more
	*/
	_getSearchSignal() {
		const yesterday = Math.floor(Date.now() / 864e5) - 1;
		let count = 0;
		if (this.storage.serpCountPrevDay === yesterday) count = this.storage.serpCountPrev || 0;
		else if (this.storage.serpCountDay === yesterday) count = this.storage.serpCount || 0;
		return count >= 10 ? "2" : count > 0 ? "1" : "0";
	}
	/**
	* Count a SERP visit for today. Rolls the current day's counter
	* into `serpCountPrev` when the UTC day changes.
	*/
	async recordSerpVisit() {
		const today = Math.floor(Date.now() / 864e5);
		if (this.storage.serpCountDay !== today) {
			this.storage.serpCountPrev = this.storage.serpCount || 0;
			this.storage.serpCountPrevDay = this.storage.serpCountDay;
			this.storage.serpCountDay = today;
			this.storage.serpCount = 1;
		} else this.storage.serpCount = (this.storage.serpCount || 0) + 1;
		await this.saveStorage(this.storage);
	}
	/**
	* Record Install event
	* @private
	*/
	_recordInstall() {
		if (this.storage.install_all) return;
		return this._sendReq("install").catch((err) => {
			this.log("Error sending metrics (\"install\" event dropped)", err);
		});
	}
	/**
	* Record Active event
	* @private
	*/
	_recordActive() {
		if (this.storage.install_all && Date.now() - this.storage.install_all < ACTIVE_PING_DELAY) return;
		const active_daily_velocity = this.storage.active_daily_velocity || [];
		const today = Math.floor(Date.now() / 864e5);
		active_daily_velocity.sort();
		if (!active_daily_velocity.includes(today)) {
			active_daily_velocity.push(today);
			if (active_daily_velocity.length > 7) active_daily_velocity.shift();
		}
		this.storage.active_daily_velocity = active_daily_velocity;
		return this.saveStorage(this.storage).then(() => this._sendReq("active", [
			"daily",
			"weekly",
			"monthly"
		])).catch((err) => {
			this.log("Error sending metrics (\"active\" event dropped)", err);
		});
	}
	/**
	* Record Install Complete event
	* @private
	*/
	_recordInstallComplete() {
		if (this.storage.install_complete_all) return;
		return this._sendReq("install_complete").catch((err) => {
			this.log("Error sending metrics (\"install_complete\" event dropped)", err);
		});
	}
	/**
	* Record Engaged event
	* @private
	*/
	_recordEngaged() {
		const engaged_daily_velocity = this.storage.engaged_daily_velocity || [];
		const engaged_daily_count = this.storage.engaged_daily_count || new Array(engaged_daily_velocity.length).fill(0);
		const today = Math.floor(Date.now() / 864e5);
		engaged_daily_velocity.sort();
		if (!engaged_daily_velocity.includes(today)) {
			engaged_daily_velocity.push(today);
			engaged_daily_count.push(1);
			if (engaged_daily_velocity.length > 7) {
				engaged_daily_count.shift();
				engaged_daily_velocity.shift();
			}
		} else engaged_daily_count[engaged_daily_velocity.indexOf(today)]++;
		this.storage.engaged_daily_count = engaged_daily_count;
		this.storage.engaged_daily_velocity = engaged_daily_velocity;
		return this.saveStorage(this.storage).then(() => this._sendReq("engaged", [
			"daily",
			"weekly",
			"monthly"
		])).catch((err) => {
			this.log("Error sending metrics (\"engaged\" event dropped)", err);
		});
	}
};
//#endregion
export { Metrics as default, processUrlQuery };
