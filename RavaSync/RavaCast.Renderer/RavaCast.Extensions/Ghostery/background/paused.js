import store_default from "../npm/hybrids/src/store.js";
import ManagedConfig from "../store/managed-config.js";
import Options from "../store/options.js";
import { addListener, isOptionEqual } from "../utils/options-observer.js";
import { PAUSED_ID_RANGE, PAUSED_RULE_PRIORITY, getDynamicRules } from "../utils/dnr.js";
//#region src/background/paused.js
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
var PAUSED_ALARM_PREFIX = "options:revoke";
chrome.alarms.onAlarm.addListener((alarm) => {
	if (alarm.name.startsWith(PAUSED_ALARM_PREFIX)) {
		const id = alarm.name.slice(15);
		store_default.set(Options, { paused: { [id]: null } });
	}
});
addListener(async function pausedSites(options, lastOptions) {
	if (options.mode !== "default") {
		if (lastOptions && options.mode !== lastOptions.mode) (await chrome.alarms.getAll()).forEach(({ name }) => {
			if (name.startsWith(PAUSED_ALARM_PREFIX)) chrome.alarms.clear(name);
		});
		return;
	}
	if (lastOptions && !isOptionEqual(options.paused, lastOptions.paused)) {
		const alarms = (await chrome.alarms.getAll()).filter(({ name }) => name.startsWith(PAUSED_ALARM_PREFIX));
		const revokeHostnames = Object.entries(options.paused).filter(([, { revokeAt }]) => revokeAt);
		alarms.forEach(({ name }) => {
			if (!revokeHostnames.find(([id]) => name === `${PAUSED_ALARM_PREFIX}:${id}`)) chrome.alarms.clear(name);
		});
		if (revokeHostnames.length) revokeHostnames.filter(([id]) => !alarms.some(({ name }) => name === `${PAUSED_ALARM_PREFIX}:${id}`)).forEach(([id, { revokeAt }]) => {
			chrome.alarms.create(`${PAUSED_ALARM_PREFIX}:${id}`, { when: revokeAt });
		});
	}
	if (lastOptions && !isOptionEqual(options.paused, lastOptions.paused) || lastOptions && options.mode !== lastOptions.mode || !lastOptions && (await store_default.resolve(ManagedConfig)).trustedDomains[0] !== "<none>") {
		const currentRules = await getDynamicRules(PAUSED_ID_RANGE);
		const hostnames = Object.keys(options.paused);
		if (hostnames.length) {
			const requestDomains = hostnames.includes("<all_urls>") ? void 0 : hostnames.sort();
			try {
				if (currentRules.length) {
					const currentRequestDomains = currentRules[0].condition.requestDomains;
					if (requestDomains === void 0 && currentRequestDomains === void 0) return;
					if (requestDomains && currentRequestDomains && currentRequestDomains.sort().join(",") === requestDomains.join(",")) return;
				}
			} catch (error) {
				console.error("[paused] Failed to compare current rules with desired state", error);
			}
			await chrome.declarativeNetRequest.updateDynamicRules({
				addRules: [{
					id: 1,
					priority: PAUSED_RULE_PRIORITY,
					action: { type: "allowAllRequests" },
					condition: {
						requestDomains,
						resourceTypes: ["main_frame"]
					}
				}],
				removeRuleIds: currentRules.map((rule) => rule.id)
			});
			console.log("[paused] Pause rules updated:", hostnames.join(", "));
		} else if (currentRules.length) {
			await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds: currentRules.map((rule) => rule.id) });
			console.log("[paused] Pause rules cleared");
		}
	}
});
//#endregion
