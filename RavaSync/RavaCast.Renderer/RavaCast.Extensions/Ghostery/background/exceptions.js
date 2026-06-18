import store_default from "../npm/hybrids/src/store.js";
import Options from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import { parseFilter } from "../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../npm/@ghostery/adblocker/dist/esm/index.js";
import { getTracker } from "../utils/trackerdb.js";
import convert from "../utils/dnr-converter.js";
import { EXCEPTIONS_ID_RANGE, EXCEPTIONS_RULE_PRIORITY, getDynamicRulesIds } from "../utils/dnr.js";
//#region src/background/exceptions.js
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
async function updateDNRRulesForExceptions() {
	const options = await store_default.resolve(Options);
	const rules = [];
	for (const [id, exception] of Object.entries(options.exceptions)) {
		const tracker = await getTracker(id) || {
			domains: [id],
			filters: []
		};
		const domains = !exception.global ? exception.domains : void 0;
		const filters = tracker.filters.concat(tracker.domains.map((domain) => `||${domain}^`)).map((f) => parseFilter(f)).filter((filter) => filter.isNetworkFilter()).map((filter) => `@@${filter.toString()}`);
		if (!filters.length) continue;
		const result = await convert(filters);
		for (const rule of result.rules) {
			if (domains && domains.length) rule.condition.initiatorDomains = domains.concat(rule.condition.initiatorDomains || []);
			rules.push({
				...rule,
				priority: EXCEPTIONS_RULE_PRIORITY + rule.priority
			});
		}
	}
	const addRules = rules.map((rule, index) => ({
		...rule,
		id: EXCEPTIONS_RULE_PRIORITY + index
	}));
	const removeRuleIds = await getDynamicRulesIds(EXCEPTIONS_ID_RANGE);
	if (addRules.length || removeRuleIds.length) {
		await chrome.declarativeNetRequest.updateDynamicRules({
			addRules,
			removeRuleIds
		});
		console.info(`[exceptions] Added ${addRules.length} network rules, removed ${removeRuleIds.length} network rules`);
	}
}
addListener("exceptions", async function(value, lastValue) {
	if (lastValue === void 0) return;
	await updateDNRRulesForExceptions();
});
//#endregion
export { updateDNRRulesForExceptions };
