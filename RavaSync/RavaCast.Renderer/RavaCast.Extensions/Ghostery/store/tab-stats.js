import store_default from "../npm/hybrids/src/store.js";
import Organization from "./organization.js";
import { getCurrentTab } from "../utils/tabs.js";
import AutoSyncingMap from "../utils/map.js";
//#region src/store/tab-stats.js
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
var Tracker = {
	id: true,
	name: "",
	category: "",
	categoryDescription: "",
	organization: Organization,
	blocked: false,
	modified: false,
	requests: [{
		url: "",
		blocked: false,
		modified: false
	}],
	requestsCount: 0,
	requestsBlocked: ({ requests }) => requests.filter((r) => r.blocked),
	requestsModified: ({ requests }) => requests.filter((r) => r.modified),
	requestsObserved: ({ requests }) => requests.filter((r) => !r.blocked && !r.modified)
};
var tab = void 0;
var TabStats = {
	domain: "",
	hostname: "",
	trackers: [Tracker],
	displayHostname: ({ hostname }) => {
		hostname = hostname.replace(/^www\./, "");
		return hostname.length > 24 ? "..." + hostname.slice(-24) : hostname;
	},
	trackersBlocked: ({ trackers }) => trackers.reduce((acc, { blocked }) => acc + Number(blocked), 0),
	trackersModified: ({ trackers }) => trackers.reduce((acc, { modified }) => acc + Number(modified), 0),
	groupedTrackers: ({ trackers }) => Object.entries(trackers.reduce((categories, tracker) => ({
		...categories,
		[tracker.category]: [...categories[tracker.category] || [], tracker]
	}), {})),
	categories: ({ trackers }) => trackers.map((t) => t.category),
	topCategories: ({ categories }) => {
		const counts = Object.entries(categories.reduce((acc, category) => {
			acc[category] = (acc[category] || 0) + 1;
			return acc;
		}, {}));
		if (counts.length < 6) return categories;
		return [...counts.slice(0, 5).map(([category, count]) => Array(count).fill(category)).flat(), ...Array(counts.slice(5).reduce((acc, [, count]) => acc + count, 0)).fill("other")];
	},
	[store_default.connect]: { async get() {
		tab ||= await getCurrentTab();
		const tabStats = await AutoSyncingMap.get("tabStats:v1", tab.id);
		if (!tabStats) throw new Error("No stats for current tab");
		return tabStats;
	} }
};
chrome.storage.onChanged.addListener((changes) => {
	if (changes["tabStats:v1"]) store_default.clear(TabStats, false);
});
//#endregion
export { TabStats as default };
