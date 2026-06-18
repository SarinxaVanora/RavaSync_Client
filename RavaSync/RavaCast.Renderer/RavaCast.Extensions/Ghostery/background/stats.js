import store_default from "../npm/hybrids/src/store.js";
import { isOpera, isWebkit } from "../utils/browser-info.js";
import Options, { getPausedDetails } from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import { order } from "../ui/categories.js";
import ExtendedRequest from "../utils/request.js";
import { getMetadata, getUnidentifiedTracker } from "../utils/trackerdb.js";
import DailyStats from "../store/daily-stats.js";
import { isSerpSupported } from "../utils/opera.js";
import AutoSyncingMap from "../utils/map.js";
import { getOffscreenImageData } from "../ui/wheel.js";
import { logRequests } from "./logger.js";
//#region src/background/stats.js
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
var tabStats = new AutoSyncingMap({ storageKey: "tabStats:v1" });
var chromeAction = chrome.action || chrome.browserAction;
var { icons } = chrome.runtime.getManifest();
Object.keys(icons).forEach((key) => {
	icons[key] = `/${icons[key]}`;
});
var inactiveIcons = Object.keys(icons).reduce((acc, key) => {
	acc[key] = icons[key].replace(".", "-inactive.");
	return acc;
}, {});
function setBadgeColor(color = "#3f4146") {
	chromeAction.setBadgeBackgroundColor({ color });
}
addListener("terms", async function stats(terms) {
	if (!terms) {
		await chromeAction.setBadgeText({ text: "!" });
		setBadgeColor("#f13436");
	} else {
		await chromeAction.setBadgeText({ text: "" });
		setBadgeColor();
	}
});
async function hasAccessToPage(tabId) {
	try {
		await chrome.scripting.insertCSS({
			target: { tabId },
			css: ""
		});
		return true;
	} catch {
		return false;
	}
}
async function refreshIcon(tabId) {
	const options = await store_default.resolve(Options);
	if (isOpera() && options.terms) isSerpSupported().then(async (supported) => {
		if (!supported) setBadgeColor(await hasAccessToPage(tabId) ? void 0 : "#f13436");
	});
	const stats = tabStats.get(tabId);
	if (!stats) return;
	const paused = !!getPausedDetails(options, stats.hostname);
	const inactive = options.mode !== "zap" && (!options.terms || paused);
	const data = {};
	if (options.trackerWheel && stats.trackers.length > 0) data.imageData = getOffscreenImageData(128, stats.trackers.map((t) => t.category), { grayscale: inactive });
	else data.path = inactive ? inactiveIcons : icons;
	chromeAction.setIcon({
		tabId,
		...data
	}, () => {
		if (chrome.runtime.lastError) console.error("setIcon failed for tabId", tabId, "(most likely the tab was closed)", chrome.runtime.lastError);
	});
	if (Options.trackerCount) try {
		await chromeAction.setBadgeText({
			tabId,
			text: options.trackerCount && (options.mode !== "zap" || !paused) ? String(stats.trackers.length) : ""
		});
	} catch (e) {
		console.error("Error while trying update the badge", e);
	}
}
var delayMap = /* @__PURE__ */ new Map();
function updateIcon(tabId, force) {
	if (delayMap.has(tabId)) {
		if (!force) return;
		clearTimeout(delayMap.get(tabId));
	}
	delayMap.set(tabId, setTimeout(() => {
		delayMap.delete(tabId);
		refreshIcon(tabId);
	}, 250));
	refreshIcon(tabId);
}
var REQUESTS_LIMIT = 100;
var OBSERVED_REQUESTS_LIMIT = 25;
async function pushTabStats(stats, requests) {
	const options = await store_default.resolve(Options);
	let trackersUpdated = false;
	logRequests(requests);
	for (const request of requests) {
		const metadata = getMetadata(request) || (request.blocked || request.modified || options.exceptions[request.hostname]) && getUnidentifiedTracker(request.hostname);
		if (metadata) {
			let tracker = stats.trackers.find((t) => t.id === metadata.id);
			if (!tracker) {
				tracker = {
					...metadata,
					requests: []
				};
				stats.trackers.push(tracker);
				trackersUpdated = true;
			}
			const savedRequest = tracker.requests.find((r) => r.url === request.url);
			if (savedRequest) {
				savedRequest.blocked = savedRequest.blocked || request.blocked;
				tracker.blocked = tracker.blocked || savedRequest.blocked;
				savedRequest.modified = savedRequest.modified || request.modified;
				tracker.modified = tracker.modified || savedRequest.modified;
			} else {
				tracker.requestsCount = (tracker.requestsCount || 0) + 1;
				tracker.blocked = tracker.blocked || request.blocked;
				tracker.modified = tracker.modified || request.modified;
				if (tracker.requests.length > REQUESTS_LIMIT) tracker.requests = tracker.requests.slice(0, REQUESTS_LIMIT - 1);
				if (request.blocked || request.modified || tracker.requests.filter((r) => !r.blocked && !r.modified).length < OBSERVED_REQUESTS_LIMIT) tracker.requests.unshift({
					id: request.requestId,
					url: request.url,
					blocked: request.blocked,
					modified: request.modified
				});
			}
		}
	}
	return trackersUpdated;
}
var deferred = Promise.resolve();
async function updateTabStats(tabId, requests) {
	await deferred;
	const stats = tabStats.get(tabId);
	if (!stats) return;
	if (stats.incognito === void 0) stats.incognito = (await chrome.tabs.get(tabId).catch(() => null))?.incognito ?? false;
	requests = requests.filter((request) => !request.sourceHostname || request.sourceHostname.endsWith(stats.hostname));
	let trackersUpdated = await pushTabStats(stats, requests);
	if (isWebkit() && chrome.declarativeNetRequest.getMatchedRules) try {
		const { rulesMatchedInfo } = await chrome.declarativeNetRequest.getMatchedRules({
			tabId,
			minTimeStamp: stats.timestamp
		});
		const notFoundRequests = [];
		for (const info of rulesMatchedInfo) {
			if (!info.request || !info.request.url) break;
			let found = false;
			for (const tracker of stats.trackers) {
				for (const request of tracker.requests) if (request.url === info.request.url) {
					found = true;
					request.blocked = true;
					tracker.blocked = true;
					break;
				}
				if (found) break;
			}
			if (!found) {
				const request = ExtendedRequest.fromRequestDetails({
					url: info.request.url,
					originUrl: stats.url
				});
				request.blocked = true;
				notFoundRequests.push(request);
			}
		}
		if (notFoundRequests.length) trackersUpdated = await pushTabStats(stats, notFoundRequests) || trackersUpdated;
	} catch (e) {
		console.error("[stats] Failed to get matched rules for stats", e);
	}
	if (stats === tabStats.get(tabId)) {
		tabStats.set(tabId, stats);
		if (trackersUpdated) {
			updateIcon(tabId);
			stats.trackers.sort((a, b) => order.indexOf(a.category) - order.indexOf(b.category));
		}
	}
}
async function flushTabStatsToDailyStats(tabId) {
	const stats = tabStats.get(tabId);
	if (!stats || !stats.trackers.length || stats.incognito) return;
	let trackersBlocked = 0;
	let trackersModified = 0;
	for (const tracker of stats.trackers) {
		trackersBlocked += tracker.blocked ? 1 : 0;
		trackersModified += tracker.modified ? 1 : 0;
	}
	const dailyStats = await store_default.resolve(DailyStats, (/* @__PURE__ */ new Date()).toISOString().split("T")[0]);
	await store_default.set(dailyStats, {
		trackersBlocked: dailyStats.trackersBlocked + trackersBlocked,
		trackersModified: dailyStats.trackersModified + trackersModified,
		pages: dailyStats.pages + 1,
		patterns: [...new Set([...dailyStats.patterns, ...stats.trackers.map((t) => t.id)])]
	});
}
var PANEL_URL = chrome.runtime.getURL("pages/panel/index.html");
function setupTabStats(details) {
	if (details.url === PANEL_URL) return;
	flushTabStatsToDailyStats(details.tabId);
	const request = ExtendedRequest.fromRequestDetails(details);
	if (request.isHttp || request.isHttps) tabStats.set(details.tabId, {
		hostname: request.hostname,
		domain: request.domain,
		url: request.url,
		trackers: [],
		timestamp: details.timeStamp
	});
	else tabStats.delete(details.tabId);
	updateIcon(details.tabId, true);
}
chrome.webNavigation.onCommitted.addListener((details) => {
	if (details.tabId > -1 && details.parentFrameId === -1) setupTabStats(details);
});
chrome.runtime.onMessage.addListener((msg, sender) => {
	if (sender.url && sender.frameId !== void 0 && sender.tab?.id > -1) switch (msg.action) {
		case "stats:update":
			updateTabStats(sender.tab.id, msg.urls.map((url) => ExtendedRequest.fromRequestDetails({
				url,
				originUrl: sender.url,
				tabId: sender.tab.id,
				requestId: Math.random().toString(36).substring(2, 20)
			})));
			break;
	}
	return false;
});
if (chrome.webRequest) {
	chrome.webRequest.onBeforeRequest.addListener((details) => {
		if (details.tabId < 0 || details.type === "main_frame") return;
		const request = ExtendedRequest.fromRequestDetails(details);
		updateTabStats(details.tabId, [request]);
	}, { urls: ["<all_urls>"] });
	chrome.webRequest.onBeforeRedirect.addListener((details) => {
		if (details.redirectUrl.startsWith("chrome-extension://")) {
			const request = ExtendedRequest.fromRequestDetails(details);
			request.blocked = true;
			updateTabStats(details.tabId, [request]);
		}
	}, { urls: ["<all_urls>"] });
	chrome.webRequest.onErrorOccurred.addListener((details) => {
		if (details.error === "net::ERR_BLOCKED_BY_CLIENT") {
			const stats = tabStats.get(details.tabId);
			if (!stats) return;
			for (const tracker of stats.trackers) for (const request of tracker.requests) if (request.id === details.requestId) {
				request.blocked = true;
				tracker.blocked = true;
			}
			const request = ExtendedRequest.fromRequestDetails(details);
			request.blocked = true;
			updateTabStats(details.tabId, [request]);
		}
	}, { urls: ["<all_urls>"] });
}
chrome.tabs.onRemoved.addListener((tabId) => {
	flushTabStatsToDailyStats(tabId);
	tabStats.delete(tabId);
});
//#endregion
export { tabStats, updateTabStats };
