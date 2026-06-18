import store_default from "../../npm/hybrids/src/store.js";
import Options, { isGloballyPaused } from "../../store/options.js";
import { addListener } from "../../utils/options-observer.js";
import { ACTION_DISABLE_GPC } from "../../npm/@ghostery/config/dist/esm/actions.js";
import Config from "../../store/config.js";
import "../../utils/request.js";
import { ALL_RESOURCE_TYPES, GPC_RULE_ID, GPC_RULE_PRIORITY, getDynamicRulesByIds } from "../../utils/dnr.js";
//#region src/background/never-consent/gpc.js
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
{
	async function updateGPCRule(options) {
		if (!options.terms || !options.blockAnnoyances || !options.autoconsent.gpc || isGloballyPaused(options)) {
			if ((await getDynamicRulesByIds([6e6])).length) {
				await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds: [GPC_RULE_ID] });
				console.log("[autoconsent] GPC rule has been removed");
			}
			return;
		}
		const config = await store_default.resolve(Config);
		const excludedDomains = [...new Set([...Object.keys(options.paused), ...Object.keys(config.domains).filter((domain) => config.hasAction(domain, ACTION_DISABLE_GPC))])];
		const existingRules = await getDynamicRulesByIds([GPC_RULE_ID]);
		if (existingRules.length) {
			const existingDomains = existingRules[0].condition.excludedInitiatorDomains || [];
			if (existingDomains.length === excludedDomains.length && existingDomains.every((d) => excludedDomains.includes(d))) return;
		}
		await chrome.declarativeNetRequest.updateDynamicRules({
			removeRuleIds: [GPC_RULE_ID],
			addRules: [{
				id: GPC_RULE_ID,
				priority: GPC_RULE_PRIORITY,
				action: {
					type: "modifyHeaders",
					requestHeaders: [{
						header: "Sec-GPC",
						operation: "set",
						value: "1"
					}]
				},
				condition: {
					...excludedDomains.length ? {
						excludedInitiatorDomains: excludedDomains,
						excludedRequestDomains: excludedDomains
					} : {},
					resourceTypes: ALL_RESOURCE_TYPES
				}
			}]
		});
		console.log("[autoconsent] GPC rule has been updated");
	}
	addListener(updateGPCRule);
	store_default.observe(Config, async (_, config, lastConfig) => {
		if (lastConfig) updateGPCRule(await store_default.resolve(Options));
	});
}
//#endregion
