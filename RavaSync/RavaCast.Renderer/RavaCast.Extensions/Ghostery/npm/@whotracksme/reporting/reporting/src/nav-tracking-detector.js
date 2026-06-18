import logger_default from "./logger.js";
import { fastHash, requireObject, requireParam, requireString } from "./utils.js";
import random from "./random.js";
import { timezoneAgnosticDailyExpireAt } from "./cooldowns.js";
//#region node_modules/@whotracksme/reporting/reporting/src/nav-tracking-detector.js
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
function tryParseHostname(url) {
	try {
		return new URL(url).host;
	} catch (e) {
		return null;
	}
}
function hasQueryParams(url, params) {
	try {
		const { searchParams } = new URL(url);
		return params.every((key) => searchParams.has(key));
	} catch (e) {
		return false;
	}
}
var searchAdRedirectByCategory = {
	go: (url) => url.startsWith("https://www.googleadservices.com/") || url.startsWith("https://www.google.com/aclk?"),
	bi: (url) => url.startsWith("https://www.bing.com/aclk?"),
	dd: (url) => url.startsWith("https://www.bing.com/aclick?") || url.startsWith("https://duckduckgo.com/y.js?") && hasQueryParams(url, [
		"ad_domain",
		"ad_provider",
		"ad_type"
	]),
	gh: (url) => url.startsWith("https://tatrck.com/h/"),
	br: (url) => url.startsWith("https://search.brave.com/a/redirect?") && hasQueryParams(url, ["click_url", "placement_id"]),
	ec: (url) => url.startsWith("https://syndicatedsearch.goog/aclk?") || url.startsWith("https://ad.doubleclick.net/searchads/link/click?")
};
function isAdUrlByCategory(url, category) {
	requireString(url);
	requireString(category);
	const check = searchAdRedirectByCategory[category];
	return !!(check && check(url));
}
function isTracking(url) {
	requireString(url);
	return Object.values(searchAdRedirectByCategory).some((check) => check(url));
}
function isSearchAdRedirect(category, redirects) {
	const trackingUrls = redirects.map((x) => x.from).filter(isTracking);
	return {
		isAd: redirects.length > 0 && isAdUrlByCategory(redirects[0].from, category) || trackingUrls.length > 0 && isAdUrlByCategory(trackingUrls[0], category),
		trackingUrls
	};
}
function toSendMessageJob(action, payload, deduplicateBy) {
	return {
		type: "send-message",
		args: {
			body: {
				action,
				payload,
				ver: 3,
				"anti-duplicates": Math.floor(random() * 1e7)
			},
			deduplicateBy
		}
	};
}
/**
* Responsible for detecting navigational tracking
* (https://privacycg.github.io/nav-tracking-mitigations/#navigational-tracking).
*
* It observes events emitted by the "Page", so some events will
* have been filtered already (e.g. "incognito" tabs are filtered out).
*/
var NavTrackingDetector = class {
	constructor({ sanitizer, persistedHashes, quorumChecker, jobScheduler }) {
		this.active = false;
		this.sanitizer = requireParam(sanitizer);
		this.persistedHashes = requireParam(persistedHashes);
		this.quorumChecker = requireParam(quorumChecker);
		this.jobScheduler = requireParam(jobScheduler);
		this.jobScheduler.registerHandler("nav-track-detect:quorum-isAdCheck", async (job) => {
			const { action, payload, deduplicateBy, quorumCheck } = job.args;
			requireString(action);
			requireObject(payload);
			requireString(quorumCheck);
			const expireAt = timezoneAgnosticDailyExpireAt();
			const dedupHash = fastHash(`nav-track:quorum:${quorumCheck}`, { truncate: true });
			if (!await this.persistedHashes.add(dedupHash, expireAt)) {
				logger_default.debug("Dropping before quorum check (already seen):", action, payload);
				return [];
			}
			try {
				if (await this._passesQuorum(quorumCheck)) return [toSendMessageJob(action, payload, deduplicateBy)];
				else {
					logger_default.debug("Dropping message (failed to reach quorum):", action, payload);
					return [];
				}
			} catch (e) {
				await this.persistedHashes.delete(dedupHash).catch(() => {});
				throw e;
			}
		}, {
			priority: -1e3,
			cooldownInMs: 3 * SECOND,
			maxJobsTotal: 200
		});
	}
	async init() {
		this.active = true;
	}
	unload() {
		this.active = false;
	}
	onPageEvent(event) {
		if (!this.active) return;
		if (event.type === "safe-page-navigation") this._analyzeNavigation(event);
		else if (event.type === "safe-search-landing") this._analyzeLanding(event.details);
	}
	_analyzeNavigation(event) {
		logger_default.debug("[STUB]: general navigation are not yet covered", event);
	}
	_analyzeLanding({ from, to, redirects }) {
		const { category, query: unsafeQuery } = from;
		const { isAd, trackingUrls } = isSearchAdRedirect(category, redirects);
		if (!isAd) return;
		const hostname = tryParseHostname(to.targetUrl);
		if (!hostname) return;
		const trackingHosts = trackingUrls.map(tryParseHostname);
		const { accept } = this.sanitizer.checkSuspiciousQuery(unsafeQuery);
		const query = accept ? unsafeQuery : null;
		const action = "wtm.nav-track-detect.search-ad";
		this._registerJob({
			type: "nav-track-detect:quorum-isAdCheck",
			args: {
				action,
				payload: {
					from: { search: {
						category,
						query
					} },
					to: { hostname },
					via: { redirects: trackingHosts }
				},
				quorumCheck: JSON.stringify([
					action,
					category,
					hostname,
					trackingHosts
				])
			}
		});
	}
	async _passesQuorum(quorumCheck) {
		requireString(quorumCheck);
		await this.quorumChecker.sendQuorumIncrement({ text: quorumCheck });
		return this.quorumChecker.checkQuorumConsent({ text: quorumCheck });
	}
	_registerJob(job) {
		this.jobScheduler.registerJob(job).catch((e) => {
			logger_default.error("Failed to register job", job, e);
		});
	}
};
//#endregion
export { NavTrackingDetector as default };
