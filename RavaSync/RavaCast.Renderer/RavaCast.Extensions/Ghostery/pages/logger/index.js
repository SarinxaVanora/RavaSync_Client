import mount from "../../npm/hybrids/src/mount.js";
import store_default from "../../npm/hybrids/src/store.js";
import "../../ui/index.js";
import log_default from "./store/log.js";
import main_default from "./views/main.js";
//#region src/pages/logger/index.js
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
chrome.runtime.connect({ name: "logger" }).onMessage.addListener(async (msg) => {
	if (msg.action === "logger:data") try {
		for (const log of msg.data) if (log.id) {
			const model = store_default.get(log_default, log.id);
			delete log.id;
			store_default.set(model, log);
		} else store_default.set(log_default, log);
	} catch (error) {
		console.error("Error processing logs:", error);
	}
});
mount(document.body, main_default);
//#endregion
