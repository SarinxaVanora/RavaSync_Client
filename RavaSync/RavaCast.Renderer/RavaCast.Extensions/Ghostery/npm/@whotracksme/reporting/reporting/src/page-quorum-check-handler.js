import logger_default from "./logger.js";
import { BadJobError } from "./errors.js";
import random from "./random.js";
//#region node_modules/@whotracksme/reporting/reporting/src/page-quorum-check-handler.js
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
function createPageMessage(safePage, ctry) {
	const { url, title, ref = null, redirects = null, search, lang } = safePage;
	const { activity } = safePage.aggregator;
	const payload = {
		url,
		t: title,
		ref,
		redirects,
		lang,
		ctry,
		activity
	};
	if (search) payload.qr = {
		q: search.query,
		t: search.category,
		d: search.depth
	};
	return {
		body: {
			action: "wtm.page",
			payload,
			ver: 3,
			"anti-duplicates": Math.floor(random() * 1e7)
		},
		deduplicateBy: "url"
	};
}
var CachedQuorumChecker = class {
	constructor(quorumChecker) {
		this.quorumChecker = quorumChecker;
		this.cached = /* @__PURE__ */ new Map();
	}
	async isQuorumReached(url) {
		const cacheHit = this.cached.get(url);
		if (cacheHit) return cacheHit;
		const pending = this.quorumChecker.checkQuorumConsent({ text: url });
		this.cached.set(url, pending);
		pending.catch((e) => {
			logger_default.warn("Quorum check for URL", url, "failed", e);
		});
		return pending;
	}
};
var PageQuorumCheckHandler = class {
	constructor({ jobScheduler, quorumChecker, countryProvider }) {
		this.quorumChecker = quorumChecker;
		this.countryProvider = countryProvider;
		jobScheduler.registerHandler("page-quorum-check", async (job) => {
			const message = await this.runJob(job.args.safePage);
			if (!message) return [];
			logger_default.debug("Page message ready:", message);
			return [{
				type: "send-message",
				args: message
			}];
		});
	}
	async runJob(safePage) {
		if (!safePage) throw new BadJobError("page information missing");
		const { url } = safePage;
		if (!url) throw new BadJobError("url missing");
		const uniqueUrlsToInc = new Set([url]);
		if (safePage.ref) uniqueUrlsToInc.add(safePage.ref);
		if (safePage.redirects) for (const { from, to } of safePage.redirects) {
			uniqueUrlsToInc.add(from);
			if (to !== "...") uniqueUrlsToInc.add(to);
		}
		const urlsToInc = [...uniqueUrlsToInc];
		const parsedUrls = new Map(urlsToInc.map((url) => {
			const normalizedUrl = url.endsWith(" (PROTECTED)") ? url.slice(0, -12) : url;
			try {
				return [url, new URL(normalizedUrl)];
			} catch (e) {
				throw new BadJobError(`Failed to parse URL (url=${url}, normalizedUrl=${normalizedUrl})`);
			}
		}));
		const isPureDomain = (url) => {
			const { pathname, search, hash } = parsedUrls.get(url);
			return pathname === "/" && !search && !hash;
		};
		for (const urlToInc of urlsToInc) if (!isPureDomain(urlToInc)) await this.quorumChecker.sendQuorumIncrement({ text: urlToInc });
		const cachedQuorumChecker = new CachedQuorumChecker(this.quorumChecker);
		if (!(safePage.search && safePage.search.depth === 1) && !isPureDomain(url) && !await cachedQuorumChecker.isQuorumReached(url)) {
			logger_default.info("Dropping page", url, "since it failed to reach quorum");
			return null;
		}
		const protectUrlIfNeeded = async (url) => {
			if (isPureDomain(url) || url === "...") return url;
			if (await cachedQuorumChecker.isQuorumReached(url)) return url;
			const parsedUrl = parsedUrls.get(url);
			return `${parsedUrl.protocol}//${parsedUrl.hostname}/ (PROTECTED)`;
		};
		if (safePage.ref) {
			safePage = { ...safePage };
			safePage.ref = await protectUrlIfNeeded(safePage.ref);
		}
		const { redirects } = safePage;
		if (redirects) {
			safePage = { ...safePage };
			safePage.redirects = await Promise.all(redirects.map(async (redirect) => {
				return {
					from: await protectUrlIfNeeded(redirect.from),
					to: await protectUrlIfNeeded(redirect.to),
					statusCode: redirect.statusCode
				};
			}));
		}
		const ctry = this.countryProvider.getSafeCountryCode();
		return createPageMessage(safePage, ctry);
	}
};
//#endregion
export { PageQuorumCheckHandler as default };
