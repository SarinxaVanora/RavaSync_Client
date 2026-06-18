import store_default from "../npm/hybrids/src/store.js";
import { isBrave } from "../utils/browser-info.js";
import Options from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import "../npm/@ghostery/config/dist/esm/flags.js";
import Config from "../store/config.js";
import { waitForConfigSync } from "./config.js";
//#region src/background/onboarding.js
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
var SURVEY_URL = "https://blocksurvey.io/install-survey-postonboarding-R6q0d5dGR9OY6202iNPmGQ?v=o";
addListener("onboarding", async (onboarding) => {
	if (onboarding) return;
	await waitForConfigSync();
	const tab = await chrome.tabs.create({ url: chrome.runtime.getURL("/pages/onboarding/index.html") });
	if (!isBrave()) chrome.tabs.onRemoved.addListener(async function listener(closedTabId) {
		if (closedTabId === tab.id) {
			chrome.tabs.onRemoved.removeListener(listener);
			const config = await store_default.resolve(Config);
			if (!(await store_default.resolve(Options)).terms || !config.hasFlag("onboarding-survey")) return;
			chrome.tabs.create({ url: SURVEY_URL });
		}
	});
});
//#endregion
export { SURVEY_URL };
