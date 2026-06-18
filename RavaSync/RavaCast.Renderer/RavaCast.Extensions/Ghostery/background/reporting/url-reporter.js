import store_default from "../../npm/hybrids/src/store.js";
import Options, { getPausedDetails } from "../../store/options.js";
import Reporting from "../../npm/@whotracksme/reporting/reporting/src/reporting.js";
import "../../npm/@whotracksme/reporting/reporting/src/index.js";
import config_default from "./config.js";
import prefixedIndexedDBKeyValueStore from "./storage-indexeddb.js";
import StorageLocal from "./storage-chrome-local.js";
import communication_default from "./communication.js";
//#region src/background/reporting/url-reporter.js
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
var url_reporter_default = new Reporting({
	config: config_default.url,
	storage: new StorageLocal("reporting"),
	connectDatabase: prefixedIndexedDBKeyValueStore("reporting"),
	communication: communication_default,
	pauseState: {
		getFilteringMode: () => store_default.get(Options).mode,
		isHostnamePaused: (hostname) => !!getPausedDetails(store_default.get(Options), hostname),
		connectHostnamePausingEvents: (notify) => {
			chrome.runtime.onMessage.addListener((msg) => {
				if (msg.action === "reporting:updateHostnamePause") {
					const { hostname, paused } = msg;
					notify({
						hostname,
						paused
					});
				}
			});
		}
	}
});
//#endregion
export { url_reporter_default as default };
