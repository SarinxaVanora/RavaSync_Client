import logger_default from "../../logger.js";
import { truncatedHash } from "../../md5.js";
import { parse } from "../../utils/url.js";
import { getGeneralDomain, sameGeneralDomain } from "../utils/tlds.js";
import ChromeStorageMap from "../utils/chrome-storage-map.js";
//#region node_modules/@whotracksme/reporting/reporting/src/request/steps/cookie-context.js
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
var TIME_AFTER_LINK = 5 * 1e3;
var TIME_CLEANING_CACHE = 180 * 1e3;
var TIME_ACTIVE = 20 * 1e3;
var UNUSED_TRUST_TIMEOUT = 120 * 1e3;
var USED_TRUST_TIMEOUT = 750 * 1e3;
var CLEAN_COOKIE_CACHE_TIMEOUT = 120 * 1e3;
var CookieContext = class {
	constructor(config, qsWhitelist) {
		this.config = config;
		this.qsWhitelist = qsWhitelist;
		this.contextFromEvent = null;
		this.visitCache = new ChromeStorageMap({
			storageKey: "wtm-request-reporting:cookie-context:visit-cache",
			ttlInMs: TIME_CLEANING_CACHE
		});
		this.trustedThirdParties = new ChromeStorageMap({
			storageKey: "wtm-request-reporting:cookie-context:trusted-third-parties",
			ttlInMs: USED_TRUST_TIMEOUT
		});
	}
	async init() {
		await this.visitCache.isReady;
		await this.trustedThirdParties.isReady;
		this.cleanCookieCache();
		this._pmclean = setInterval(this.cleanCookieCache.bind(this), CLEAN_COOKIE_CACHE_TIMEOUT);
	}
	unload() {
		clearInterval(this._pmclean);
		this._pmclean = null;
	}
	cleanCookieCache() {
		const now = Date.now();
		this.trustedThirdParties.forEach((counter, key) => {
			if (now > counter.ts + (counter.c > 0 ? USED_TRUST_TIMEOUT : UNUSED_TRUST_TIMEOUT)) this.trustedThirdParties.delete(key);
		});
	}
	_addTrustLink(fromFirstParty, toThirdParty) {
		if (sameGeneralDomain(fromFirstParty, toThirdParty)) return;
		if (this.qsWhitelist.isTrackerDomain(truncatedHash(getGeneralDomain(toThirdParty)))) return;
		const key = `${fromFirstParty}:${toThirdParty}`;
		if (!this.trustedThirdParties.has(key)) this.trustedThirdParties.set(key, { c: 0 });
		this.trustedThirdParties.get(key).ts = Date.now();
	}
	assignCookieTrust(state) {
		if (state.isMainFrame && state.getReferrer()) {
			const referrer = parse(state.getReferrer());
			if (!referrer) return true;
			const trustedHost = state.urlParts.hostname;
			const trustedOn = referrer.hostname;
			this._addTrustLink(trustedOn, trustedHost);
		}
		return true;
	}
	checkCookieTrust(state) {
		const stage = state.statusCode !== void 0 ? "set_cookie" : "cookie";
		const key = `${state.tabUrlParts.hostname}:${state.urlParts.hostname}`;
		if (this.config.cookieTrustReferers && this.trustedThirdParties.has(key)) {
			const trustCounter = this.trustedThirdParties.get(key);
			trustCounter.c += 1;
			trustCounter.ts = Date.now();
			state.incrementStat(`${stage}_allow_trust`);
			return false;
		}
		return true;
	}
	checkVisitCache(state) {
		const stage = state.statusCode !== void 0 ? "set_cookie" : "cookie";
		const tabId = state.tabId;
		if (Date.now() - (this.visitCache.get(`${tabId}:${state.hostGD}`) || 0) < TIME_ACTIVE && this.visitCache.get(`${tabId}:${state.sourceGD}`)) {
			state.incrementStat(`${stage}_allow_visitcache`);
			return false;
		}
		return true;
	}
	checkContextFromEvent(state) {
		if (this.contextFromEvent) {
			const stage = state.statusCode !== void 0 ? "set_cookie" : "cookie";
			const time = Date.now();
			const url = state.url;
			const tabId = state.tabId;
			const urlParts = state.urlParts;
			const sourceGD = state.tabUrlParts.generalDomain;
			const hostGD = state.urlParts.generalDomain;
			if (time - (this.contextFromEvent.ts || 0) < TIME_AFTER_LINK) {
				if (hostGD === this.contextFromEvent.cGD && sourceGD === this.contextFromEvent.pageGD) {
					this.visitCache.set(`${tabId}:${hostGD}`, time);
					state.incrementStat(`${stage}_allow_userinit_same_context_gd`);
					return false;
				}
				const pu = url.split(/[?&;]/)[0];
				if (this.contextFromEvent.html.indexOf(pu) !== -1) {
					if (urlParts && urlParts.hostname && urlParts.hostname !== "") {
						this.visitCache.set(`${tabId}:${hostGD}`, time);
						state.incrementStat(`${stage}_allow_userinit_same_gd_link`);
						return false;
					}
				}
				if (!this.contextFromEvent.cGD && this.contextFromEvent.possibleCGD.has(hostGD)) {
					this.visitCache.set(`${tabId}:${hostGD}`, time);
					state.incrementStat(`${stage}_allow_userinit_same_script_gd`);
					return false;
				}
			}
		}
		return true;
	}
	extractPossilbeContextGD(links) {
		return new Set(links.map((link) => parse(link).generalDomain));
	}
	setContextFromEvent(ev, contextHTML, herf, sender) {
		let cGD = null;
		let pageGD = null;
		const html = contextHTML || "";
		try {
			pageGD = parse(sender.tab.url).generalDomain;
			cGD = parse(ev.target.baseURI).generalDomain;
		} catch (ee) {
			logger_default.error("CookieContext could not parse URL", ee);
		}
		if (!pageGD || cGD === pageGD) return;
		const possibleCGD = this.extractPossilbeContextGD(ev.target.linksSrc);
		this.contextFromEvent = {
			html,
			ts: Date.now(),
			cGD,
			pageGD,
			possibleCGD
		};
	}
};
//#endregion
export { CookieContext as default };
