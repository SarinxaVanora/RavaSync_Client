import store_default from "../npm/hybrids/src/store.js";
import Options from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import { parseFilters } from "../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../npm/@ghostery/adblocker/dist/esm/index.js";
import { DISTRACTIONS_ENGINE, create, getConfig, init, remove } from "../utils/engines.js";
import convert from "../utils/dnr-converter.js";
import { DISTRACTIONS_ID_RANGE, getDynamicRulesIds } from "../utils/dnr.js";
import { reloadMainEngine } from "./adblocker/engines.js";
import { xxh32 } from "../npm/minixxh/out/xxh32.js";
//#region src/background/distractions.js
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
var FILTERS_URL = chrome.runtime.getURL("background/rule_resources/distractions.json");
async function fetchFilters() {
	return (await fetch(FILTERS_URL)).json();
}
function getFiltersChecksum(filters) {
	const bytes = new TextEncoder().encode(Object.values(filters).map((filters) => filters.join("\n")).join("\n"));
	return xxh32(bytes, 0, bytes.length).toString(16);
}
async function updateDistractions(distractions) {
	const filters = await fetchFilters();
	const enabledFilters = Object.entries(distractions).filter(([, enabled]) => enabled).flatMap(([id]) => filters[id] || []);
	if (enabledFilters.length === 0) {
		remove(DISTRACTIONS_ENGINE);
		console.info("[distractions] Engine removed...");
		{
			const removeRuleIds = await getDynamicRulesIds(DISTRACTIONS_ID_RANGE);
			if (removeRuleIds.length) {
				await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds });
				console.info("[distractions] DNR rules removed...");
			}
		}
		return;
	}
	const baseConfig = await getConfig();
	const { networkFilters, cosmeticFilters, preprocessors } = parseFilters(enabledFilters.join("\n"), {
		...baseConfig,
		debug: true
	});
	{
		const removeRuleIds = await getDynamicRulesIds(DISTRACTIONS_ID_RANGE);
		if (removeRuleIds.length) await chrome.declarativeNetRequest.updateDynamicRules({ removeRuleIds });
		if (networkFilters.length) {
			const { rules } = await convert(networkFilters.map((f) => f.rawLine));
			const dnrRules = rules.map((rule, index) => ({
				...rule,
				id: DISTRACTIONS_ID_RANGE.start + index
			}));
			await chrome.declarativeNetRequest.updateDynamicRules({ addRules: dnrRules });
			console.info(`[distractions] DNR updated with ${dnrRules.length} rule(s)`);
		}
	}
	await create(DISTRACTIONS_ENGINE, {
		networkFilters,
		cosmeticFilters,
		preprocessors,
		lists: { filters: getFiltersChecksum(filters) }
	});
	console.info(`[distractions] Engine updated with ${cosmeticFilters.length} filter(s)`);
	await reloadMainEngine();
}
chrome.runtime.onInstalled.addListener(async () => {
	try {
		const engine = await init(DISTRACTIONS_ENGINE);
		if (!engine || engine.lists.get("filters") === getFiltersChecksum(await fetchFilters())) return;
		const { distractions } = await store_default.resolve(Options);
		await updateDistractions(distractions);
	} catch (e) {
		console.error("[distractions] Failed to update engine on install", e);
	}
});
addListener("distractions", async (value, lastValue) => {
	try {
		if (!lastValue) {
			if (!Object.values(value).some(Boolean)) return;
			if (await init("distractions")) return;
		}
		await updateDistractions(value);
	} catch (e) {
		console.error("[distractions] Failed to update engine", e);
	}
});
//#endregion
