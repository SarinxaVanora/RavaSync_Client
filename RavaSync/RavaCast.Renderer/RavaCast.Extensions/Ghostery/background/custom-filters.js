import store_default from "../npm/hybrids/src/store.js";
import CustomFilters from "../store/custom-filters.js";
import ManagedConfig from "../store/managed-config.js";
import Options from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import { FilterType, parseFilters } from "../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../npm/@ghostery/adblocker/dist/esm/index.js";
import { CUSTOM_ENGINE, create, getConfig, init } from "../utils/engines.js";
import convert from "../utils/dnr-converter.js";
import { CUSTOM_FILTERS_ID_RANGE, getDynamicRulesIds } from "../utils/dnr.js";
import { reloadMainEngine, setup } from "./adblocker/engines.js";
import { updateRedirectProtectionRules } from "./redirect-protection.js";
//#region src/background/custom-filters.js
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
function isTrustedScriptInject(scriptName) {
	return scriptName === "rpnt" || scriptName === "replace-node-text" || scriptName.startsWith("trusted-");
}
async function updateDNRRules(dnrRules) {
	const removeRuleIds = await getDynamicRulesIds(CUSTOM_FILTERS_ID_RANGE);
	if (removeRuleIds.length) await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds });
	if (dnrRules.length) {
		dnrRules = dnrRules.map((rule, index) => ({
			...rule,
			id: CUSTOM_FILTERS_ID_RANGE.start + index
		}));
		await chrome.declarativeNetRequest.updateDynamicRules({ addRules: dnrRules });
		console.info(`[custom filters] DNR updated with rules: ${dnrRules.length}`);
	}
	if (removeRuleIds.length || dnrRules.length) await updateRedirectProtectionRules(await store_default.resolve(Options));
	return dnrRules;
}
function findLineNumber(text, line) {
	const index = text.indexOf(line);
	if (index === -1) return -1;
	if (index === 0) return 1;
	let lines = 1;
	for (let i = 0; i < index; i++) if (text.charCodeAt(i) === 10 || text.charCodeAt(i) === 13) lines++;
	return lines;
}
async function collectFilters(text, { isTrustedScriptInjectAllowed }) {
	const { networkFilters, cosmeticFilters, preprocessors, notSupportedFilters } = parseFilters(text, {
		...await getConfig(),
		debug: true
	});
	const errors = notSupportedFilters.reduce(function(state, { filter, filterType, lineNumber }) {
		if (filterType !== FilterType.NOT_SUPPORTED_EMPTY && filterType !== FilterType.NOT_SUPPORTED_COMMENT) state.push(`Filter not supported (${lineNumber + 1}): ${filter}`);
		return state;
	}, []);
	const acceptedCosmeticFilters = cosmeticFilters.filter(function(filter) {
		if (filter.isScriptInject() === false || isTrustedScriptInjectAllowed === true) return true;
		const scriptNameIndex = filter.selector.indexOf(",");
		if (isTrustedScriptInject(scriptNameIndex === -1 ? filter.selector : filter.selector.slice(0, scriptNameIndex))) {
			errors.push(`Trusted scriptlets are not allowed (${findLineNumber(text, filter.rawLine)}): ${filter.rawLine}`);
			return false;
		}
		return true;
	});
	/**
	* @type {Map<number, string>}
	*/
	let filterIdToRawLine = null;
	filterIdToRawLine = /* @__PURE__ */ new Map();
	for (const filter of networkFilters) filterIdToRawLine.set(filter.getId(), filter.rawLine);
	return {
		networkFilters,
		filterIdToRawLine,
		cosmeticFilters: acceptedCosmeticFilters,
		preprocessors,
		errors
	};
}
async function updateEngine({ networkFilters, cosmeticFilters, preprocessors }) {
	const engine = await create(CUSTOM_ENGINE, {
		networkFilters,
		cosmeticFilters,
		preprocessors
	});
	console.info(`[custom filters] Engine updated with network filters: ${networkFilters.length}, cosmetic filters: ${cosmeticFilters.length}`);
	return engine;
}
async function updateCustomFilters(input, options) {
	setup.pending && await setup.pending;
	const { networkFilters, cosmeticFilters, preprocessors, errors, filterIdToRawLine } = await collectFilters(input, { isTrustedScriptInjectAllowed: options.trustedScriptlets });
	const engine = await updateEngine({
		networkFilters,
		cosmeticFilters,
		preprocessors
	});
	await reloadMainEngine();
	const result = {
		networkFilters: networkFilters.length,
		cosmeticFilters: cosmeticFilters.length,
		errors
	};
	{
		const { rules, errors } = await convert(engine.getFilters().networkFilters.filter(function(filter) {
			return engine.preprocessors.isFilterExcluded(filter) === false;
		}).map(function(filter) {
			return filterIdToRawLine.get(filter.getId());
		}));
		result.dnrRules = await updateDNRRules(rules);
		if (errors?.length) result.errors.push(...errors);
	}
	return result;
}
addListener("customFilters", async (value, lastValue) => {
	if (value.enabled) {
		const managedConfig = await store_default.resolve(ManagedConfig);
		if (managedConfig.customFilters.enabled) {
			const currentText = (await store_default.resolve(CustomFilters)).text;
			const text = managedConfig.customFilters.filters.join("\n");
			if (text !== currentText) {
				await store_default.set(CustomFilters, { text });
				await updateCustomFilters(text, value);
				return;
			}
		}
	}
	if (!lastValue && value.enabled && !await init("custom-filters")) {
		const { text } = await store_default.resolve(CustomFilters);
		await updateCustomFilters(text, value);
	} else if (lastValue && value.trustedScriptlets === lastValue.trustedScriptlets) if (value.enabled) {
		const { text } = await store_default.resolve(CustomFilters);
		await updateCustomFilters(text, value);
	} else {
		await reloadMainEngine();
		await updateDNRRules([]);
	}
});
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
	if (msg.action === "customFilters:update") {
		store_default.resolve(Options).then((options) => {
			updateCustomFilters(msg.input, options.customFilters).then(sendResponse);
		});
		return true;
	}
});
//#endregion
