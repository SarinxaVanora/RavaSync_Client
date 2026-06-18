import mount from "../../npm/hybrids/src/mount.js";
import store_default from "../../npm/hybrids/src/store.js";
import ManagedConfig from "../../store/managed-config.js";
import Options from "../../store/options.js";
import "../../ui/index.js";
import Config from "../../store/config.js";
import settings_default from "./settings.js";
import "./elements.js";
/* empty css           */
//#region src/pages/settings/index.js
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
Promise.all([
	store_default.resolve(Options),
	store_default.resolve(ManagedConfig),
	store_default.resolve(Config)
]).then(([{ terms }, managedConfig]) => {
	if (!terms || managedConfig.disableUserControl) throw new Error("Access denied");
	chrome.runtime.sendMessage({ action: "syncOptions" });
	mount(document.body, settings_default);
	setInterval(() => chrome.runtime.sendMessage({ action: "keepAlive" }), 15e3);
}).catch(() => {
	window.location.replace(chrome.runtime.getURL("/pages/onboarding/index.html"));
});
//#endregion
