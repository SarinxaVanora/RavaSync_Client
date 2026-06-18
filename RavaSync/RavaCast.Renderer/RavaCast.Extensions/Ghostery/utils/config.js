import { getBrowser, isWebkit } from "./browser-info.js";
//#region src/utils/config.js
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
var SUPPORTED_FILTERS = [
	"platform",
	"browser",
	"version",
	"minVersion",
	"maxVersion"
];
/**
* Compare two version strings.
* Returns -1 if a < b, 0 if a === b, 1 if a > b.
*/
function compareVersions(a, b) {
	const partsA = a.split(".").map((n) => parseInt(n, 10));
	const partsB = b.split(".").map((n) => parseInt(n, 10));
	const length = Math.max(partsA.length, partsB.length);
	for (let i = 0; i < length; i += 1) {
		const va = partsA[i] ?? 0;
		const vb = partsB[i] ?? 0;
		if (va > vb) return 1;
		if (va < vb) return -1;
	}
	return 0;
}
function filter(item) {
	if (item.filter) {
		let check = Object.keys(item.filter).every((key) => {
			if (!SUPPORTED_FILTERS.includes(key)) {
				console.warn(`[config] Unsupported filter key: ${key}`);
				return false;
			}
			return true;
		});
		if (check && Array.isArray(item.filter.platform)) {
			let platform;
			platform = isWebkit() ? "webkit" : "chromium";
			check = item.filter.platform.includes(platform);
		}
		if (check && typeof item.filter.browser === "string") check = getBrowser().name === item.filter.browser;
		const extensionVersion = chrome.runtime.getManifest().version;
		if (check && typeof item.filter.version === "string") item.filter.minVersion = item.filter.version;
		if (check && typeof item.filter.minVersion === "string") check = compareVersions(extensionVersion, item.filter.minVersion) >= 0;
		if (check && typeof item.filter.maxVersion === "string") check = compareVersions(extensionVersion, item.filter.maxVersion) <= 0;
		return check;
	}
	return true;
}
//#endregion
export { filter };
