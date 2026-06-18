import { GHOSTERY_DOMAIN } from "./urls.js";
//#region src/utils/opera.js
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
var TEST_COOKIE_NAME = `ghostery:opera:cookie:test:${Date.now()}`;
var COOKIE_DOMAIN = `.${GHOSTERY_DOMAIN}`;
var COOKIE_URL = `https://${GHOSTERY_DOMAIN}`;
var isSupported = void 0;
async function isSerpSupported() {
	if (isSupported === void 0) try {
		await chrome.cookies.set({
			url: "https://www.google.com/",
			name: TEST_COOKIE_NAME,
			value: "",
			domain: ".google.com",
			path: "/",
			secure: true,
			httpOnly: true
		});
		chrome.cookies.remove({
			url: "https://www.google.com/",
			name: TEST_COOKIE_NAME
		});
		chrome.cookies.set({
			name: "opera_serp_notification",
			url: COOKIE_URL,
			path: "/",
			value: "true",
			domain: COOKIE_DOMAIN,
			expirationDate: Date.now() / 1e3 + 3600 * 24 * 365 * 10,
			httpOnly: true
		});
		isSupported = true;
	} catch {
		isSupported = false;
	}
	return isSupported;
}
//#endregion
export { isSerpSupported };
