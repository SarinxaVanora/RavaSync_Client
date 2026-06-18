import store_default from "../npm/hybrids/src/store.js";
import Options from "../store/options.js";
import { WHATS_NEW_PAGE_URL } from "../utils/urls.js";
import { openNotification } from "./notifications.js";
//#region src/background/whats-new.js
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
var { version } = chrome.runtime.getManifest();
chrome.runtime.onStartup.addListener(async () => {
	console.log("[whats-new] Checking for new minor version...");
	const options = await store_default.resolve(Options);
	const whatsNewVersion = parseFloat(version);
	if (options.whatsNewVersion === whatsNewVersion) return;
	if (options.whatsNewVersion === 0) {
		store_default.set(options, { whatsNewVersion });
		return;
	}
	const tabs = await chrome.tabs.query({ currentWindow: true });
	const activeTab = tabs.find((tab) => tab.active);
	if (tabs.length && activeTab?.url.startsWith("http")) {
		if (await openNotification({
			id: "whats-new",
			tabId: activeTab.id,
			position: "center",
			params: { whatsNewVersion }
		})) await store_default.set(options, { whatsNewVersion });
	} else if (tabs.length <= 1 && !activeTab?.url.startsWith("http")) {
		await chrome.tabs.create({
			url: WHATS_NEW_PAGE_URL,
			active: true
		});
		await store_default.set(options, { whatsNewVersion });
	}
});
//#endregion
