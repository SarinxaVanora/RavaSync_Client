import store_default from "../npm/hybrids/src/store.js";
import Options, { isGloballyPaused } from "../store/options.js";
import { parseWithCache } from "../utils/request.js";
import { getWTMStats } from "../utils/wtm-stats.js";
import trackers_preview_default from "../content_scripts/trackers-preview.css.js";
import { recordSerpVisit } from "./telemetry/index.js";
//#region src/background/serp.js
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
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
	if (msg.action === "getWTMReport") sendResponse({ wtmStats: msg.links.map((url) => {
		const { domain } = parseWithCache(url);
		return {
			stats: getWTMStats(domain),
			domain
		};
	}) });
	if (msg.action === "disableWTMReport") store_default.set(Options, { wtmSerpReport: false });
	return false;
});
var SERP_URL_REGEXP = /^https?:[/][/][^/]*[.](google|bing)[.][a-z]+([.][a-z]+)?([/][a-z]+)*[/]search/;
chrome.webNavigation.onCommitted.addListener(async (details) => {
	if (details.url.match(SERP_URL_REGEXP)) {
		recordSerpVisit();
		const options = await store_default.resolve(Options);
		if (options.wtmSerpReport) chrome.scripting.insertCSS({
			target: { tabId: details.tabId },
			css: trackers_preview_default
		});
		if (options.wtmSerpReport || options.serpTrackingPrevention) {
			const files = [];
			if (options.wtmSerpReport) files.push("/content_scripts/trackers-preview.js");
			if (!isGloballyPaused(options) && options.serpTrackingPrevention) files.push("/content_scripts/prevent-serp-tracking.js");
			if (files.length === 0) return;
			chrome.scripting.executeScript({
				injectImmediately: true,
				world: chrome.scripting.ExecutionWorld?.ISOLATED ?? "ISOLATED",
				target: { tabId: details.tabId },
				files
			}, () => {
				if (chrome.runtime.lastError) console.error(chrome.runtime.lastError);
			});
		}
	}
});
//#endregion
