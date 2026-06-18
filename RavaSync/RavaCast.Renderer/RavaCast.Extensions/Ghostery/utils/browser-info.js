import Bowser from "../npm/bowser/src/bowser.js";
//#region src/utils/browser-info.js
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
var ua;
function getUA() {
	if (ua) return ua;
	ua = Bowser.parse(navigator.userAgent);
	return ua;
}
function getBrowser() {
	{
		if (navigator.brave?.isBrave) return {
			name: "brave",
			token: "br"
		};
		if (navigator.userAgent.includes("OculusBrowser")) return {
			name: "oculus",
			token: "oc"
		};
		const browser = getUA().browser.name;
		if (browser.includes("Safari")) return {
			name: "safari",
			token: "sf"
		};
		if (browser.includes("Chrome")) return {
			name: "chrome",
			token: "ch"
		};
		if (browser.includes("Edge")) return {
			name: "edge",
			token: "ed"
		};
		if (browser.includes("Opera")) return {
			name: "opera",
			token: "op"
		};
		if (browser.includes("Yandex")) return {
			name: "yandex",
			token: "yx"
		};
		return {
			name: browser.toLowerCase().replace(/\s+/g, "_"),
			token: ""
		};
	}
}
function isBrave() {
	return getBrowser().name === "brave";
}
function isFirefox() {
	return getBrowser().name === "firefox";
}
function isEdge() {
	return getBrowser().name === "edge";
}
function isOpera() {
	return getBrowser().name === "opera";
}
function isSafari() {
	return getBrowser().name === "safari";
}
function isWebkit() {
	if (isSafari() || getOS() === "ios") return true;
	return false;
}
function getOS() {
	const platform = getUA().os?.name?.toLowerCase() || "";
	if (platform.includes("mac")) return "mac";
	else if (platform.includes("win")) return "win";
	else if (platform.includes("android")) return "android";
	else if (platform.includes("ios")) return "ios";
	else if (platform.includes("chromium os")) return "cros";
	else if (platform.includes("bsd")) return "openbsd";
	else if (platform.includes("linux")) return "linux";
	return "other";
}
function isMobile() {
	const os = getOS();
	return os === "android" || os === "ios";
}
var browserInfo = null;
async function getBrowserInfo() {
	if (browserInfo) return browserInfo;
	const ua = getUA();
	const { name, token } = getBrowser();
	browserInfo = {
		name,
		token,
		version: parseInt(ua.browser.version, 10),
		os: getOS(),
		osVersion: ua.os.version || ""
	};
	if (browserInfo.os === "mac" && chrome.runtime.getPlatformInfo && (await chrome.runtime.getPlatformInfo()).os === "ios") browserInfo.os = "ipados";
	return browserInfo;
}
//#endregion
export { getBrowserInfo as default, getBrowser, getOS, isBrave, isEdge, isFirefox, isMobile, isOpera, isSafari, isWebkit };
