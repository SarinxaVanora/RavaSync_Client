//#region src/background/reporting/config.js
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
var URL_INFIX = "";
function platformSpecificSettings() {
	if (/iPad|iPhone|iPod/.test(navigator.platform) || navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"de",
			"fr"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-safari-ios/patterns.json`,
		CHANNEL: "safari-ios"
	};
	if (/Safari/i.test(navigator.userAgent) && /Apple Computer/.test(navigator.vendor) && !/Mobi|Android/i.test(navigator.userAgent)) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"de",
			"fr"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-safari-desktop/patterns.json`,
		CHANNEL: "safari-desktop"
	};
	if (navigator.userAgent.includes("Android")) {
		if (navigator.userAgent.includes("Chrome/")) return {
			ALLOWED_COUNTRY_CODES: [
				"us",
				"de",
				"fr"
			],
			PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-chrome-android/patterns.json`,
			CHANNEL: "android"
		};
		return {
			ALLOWED_COUNTRY_CODES: [
				"us",
				"de",
				"fr",
				"gb",
				"jp",
				"ca",
				"nl",
				"pl",
				"it",
				"au",
				"es",
				"in",
				"at",
				"ch",
				"se",
				"fi",
				"be",
				"br",
				"mx"
			],
			PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-firefox-android/patterns.json`,
			CHANNEL: "android"
		};
	}
	if (navigator.userAgent.includes("Opera") || navigator.userAgent.includes("OPR") || navigator.userAgent.includes("YaBrowser")) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"de",
			"ru",
			"fr",
			"pl",
			"gb",
			"br",
			"ca",
			"ua",
			"nl",
			"es"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-opera-desktop/patterns.json`,
		CHANNEL: "opera"
	};
	if (navigator.userAgent.includes("Edg/")) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"de",
			"ru",
			"fr",
			"pl",
			"gb",
			"br",
			"ca",
			"ua",
			"nl",
			"es",
			"jp",
			"cn",
			"pt",
			"it",
			"hu"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-edge-desktop/patterns.json`,
		CHANNEL: "edge-desktop"
	};
	if (navigator.userAgent.includes("Firefox/")) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"fr",
			"de",
			"gb",
			"jp",
			"nl",
			"ca",
			"in",
			"ru",
			"it",
			"pl",
			"au",
			"es",
			"br",
			"id",
			"be",
			"ua",
			"mx",
			"ar",
			"ch",
			"at",
			"se",
			"dk",
			"hu",
			"tr",
			"gr",
			"cz",
			"ph",
			"tw",
			"ro",
			"kr",
			"fi",
			"cn",
			"no",
			"pt",
			"sg",
			"nz"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-firefox-desktop/patterns.json`,
		CHANNEL: "firefox-desktop"
	};
	if (navigator.userAgent.includes("Chrome/")) return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"fr",
			"de",
			"gb",
			"jp",
			"nl",
			"ca",
			"in",
			"ru",
			"it",
			"pl",
			"au",
			"es",
			"br",
			"id",
			"be",
			"ua",
			"mx",
			"ar",
			"ch",
			"at",
			"se",
			"dk",
			"hu",
			"tr",
			"gr",
			"cz",
			"ph",
			"tw",
			"ro",
			"kr",
			"fi",
			"cn",
			"no",
			"pt",
			"sg",
			"nz",
			"ng"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-chrome-desktop/patterns.json`,
		CHANNEL: "chrome-desktop"
	};
	console.warn("No matching config found. Falling back to patterns from Chrome Desktop.");
	return {
		ALLOWED_COUNTRY_CODES: [
			"us",
			"de",
			"fr"
		],
		PATTERNS_URL: `https://cdn2.ghostery.com/${URL_INFIX}wtm-chrome-desktop/patterns.json`,
		CHANNEL: "chrome-desktop"
	};
}
var COLLECTOR_DIRECT_URL = "https://anonymous-communication.ghostery.net";
var config_default = {
	url: {
		COLLECTOR_DIRECT_URL,
		COLLECTOR_PROXY_URL: COLLECTOR_DIRECT_URL,
		CONFIG_URL: "https://api.ghostery.net/api/v1/config",
		SAFE_QUORUM_CONFIG_ENDPOINT: "https://safe-browsing-quorum.privacy.ghostery.net/config",
		...platformSpecificSettings()
	},
	request: {
		configUrl: "https://cdn.ghostery.com/antitracking/config.json",
		remoteWhitelistUrl: "https://cdn.ghostery.com/antitracking/whitelist/2",
		localWhitelistUrl: "/rule_resources/whotracksme"
	}
};
//#endregion
export { config_default as default };
