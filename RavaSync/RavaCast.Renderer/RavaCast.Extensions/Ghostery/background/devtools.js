import store_default from "../npm/hybrids/src/store.js";
import Config from "../store/config.js";
import { deleteDatabases } from "../utils/indexeddb.js";
//#region src/background/devtools.js
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
	switch (msg.action) {
		case "clearStorage":
			(async () => {
				try {
					console.info("[devtools] Clearing main local storage");
					chrome.storage.local.clear();
					console.info("[devtools] Clearing sync storage");
					chrome.storage.sync.clear();
					console.info("[devtools] Removing all indexedDBs...");
					await deleteDatabases().catch((e) => {
						console.error("[devtools] Error removing indexedDBs:", e);
					});
					chrome.tabs.remove(sender.tab.id);
					chrome.runtime.reload();
				} catch (e) {
					sendResponse(`[devtools] Error clearing storage: ${e}`);
				}
			})();
			return true;
		case "devtools:config":
			(async () => {
				try {
					await store_default.set(Config, msg.values);
					sendResponse("Config updated");
				} catch (e) {
					sendResponse(`[devtools] Error syncing config: ${e}`);
				}
			})();
			return true;
	}
	return false;
});
//#endregion
