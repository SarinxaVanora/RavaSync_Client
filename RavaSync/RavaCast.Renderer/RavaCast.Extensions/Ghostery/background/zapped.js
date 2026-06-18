import store_default from "../npm/hybrids/src/store.js";
import { isWebkit } from "../utils/browser-info.js";
import Options, { MODE_DEFAULT } from "../store/options.js";
import { addListener, isOptionEqual } from "../utils/options-observer.js";
import "../npm/@ghostery/config/dist/esm/flags.js";
import Config from "../store/config.js";
import { PAUSED_ID_RANGE, PAUSED_RULE_PRIORITY, getDynamicRulesIds } from "../utils/dnr.js";
//#region src/background/zapped.js
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
store_default.observe(Config, async (_, config, lastConfig) => {
	if (lastConfig?.hasFlag("modes") && !config.hasFlag("modes")) {
		const removeRuleIds = await getDynamicRulesIds(PAUSED_ID_RANGE);
		await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds });
		await store_default.set(Options, {
			mode: MODE_DEFAULT,
			zapped: null
		});
		console.log(`[zapped] Filtering mode flag removed, resetting filtering mode and zapped data`);
	}
});
addListener(async function zapped(options, lastOptions) {
	if (!lastOptions || options.mode !== "zap" || options.mode === lastOptions.mode && isOptionEqual(options.zapped, lastOptions.zapped)) return;
	const removeRuleIds = await getDynamicRulesIds(PAUSED_ID_RANGE);
	const excludedDomains = Object.keys(options.zapped);
	await chrome.declarativeNetRequest.updateDynamicRules({
		addRules: [isWebkit() ? {
			id: 1,
			priority: PAUSED_RULE_PRIORITY,
			action: { type: "allow" },
			condition: { excludedInitiatorDomains: excludedDomains }
		} : {
			id: 1,
			priority: PAUSED_RULE_PRIORITY,
			action: { type: "allowAllRequests" },
			condition: {
				excludedRequestDomains: excludedDomains,
				resourceTypes: ["main_frame"]
			}
		}],
		removeRuleIds
	});
	console.log(`[zapped] Zap mode rules updated`);
});
//#endregion
