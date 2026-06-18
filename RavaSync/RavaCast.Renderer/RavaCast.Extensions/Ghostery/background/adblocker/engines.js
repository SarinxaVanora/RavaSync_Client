import store_default from "../../npm/hybrids/src/store.js";
import { isWebkit } from "../../utils/browser-info.js";
import Options, { ENGINES } from "../../store/options.js";
import { addListener } from "../../utils/options-observer.js";
import { CUSTOM_ENGINE, DISTRACTIONS_ENGINE, ELEMENT_PICKER_ENGINE, FIXES_ENGINE, MAIN_ENGINE, create, init, isPersistentEngine, replace, update } from "../../utils/engines.js";
import asyncSetup from "../../utils/setup.js";
import { setup as setup$1 } from "../../utils/trackerdb.js";
import { updateDNRRulesForExceptions } from "../exceptions.js";
import "./content-scripts.js";
//#region src/background/adblocker/engines.js
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
function getEnabledEngines(options) {
	if (options.terms) {
		const list = ENGINES.filter(({ key }) => options[key]).map(({ name }) => name);
		if (options.regionalFilters.enabled) list.push(...options.regionalFilters.regions.map((id) => `lang-${id}`));
		if (options.fixesFilters && list.length) list.push(FIXES_ENGINE);
		list.push(ELEMENT_PICKER_ENGINE);
		if (options.customFilters.enabled) list.push(CUSTOM_ENGINE);
		if (Object.values(options.distractions).some(Boolean)) list.push(DISTRACTIONS_ENGINE);
		return list;
	}
	return [];
}
function pause(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
async function reloadMainEngine() {
	if (isWebkit()) await pause(1e3);
	const enabledEngines = getEnabledEngines(await store_default.resolve(Options));
	const resolvedEngines = (await Promise.all(enabledEngines.map((id) => init(id).catch(() => {
		console.error(`[adblocker] failed to load engine: ${id}`);
		return null;
	}).then((engine) => {
		if (!engine) enabledEngines.splice(enabledEngines.indexOf(id), 1);
		return engine;
	})))).filter((engine) => engine);
	if (resolvedEngines.length) {
		replace(MAIN_ENGINE, resolvedEngines);
		console.info(`[adblocker] Main engine reloaded with: ${enabledEngines.join(", ")}`);
	} else {
		await create(MAIN_ENGINE);
		console.info("[adblocker] Main engine reloaded with no filters");
	}
}
var updating = false;
async function updateEngines({ cache = true } = {}) {
	if (updating) return;
	try {
		updating = true;
		const enabledEngines = getEnabledEngines(await store_default.resolve(Options));
		if (enabledEngines.length) {
			let updated = false;
			await Promise.all(enabledEngines.filter(isPersistentEngine).map(async (id) => {
				await init(id);
				updated = await update(id, { cache }) || updated;
			}));
			setup$1.pending && await setup$1.pending;
			if (await update("trackerdb", { cache })) await updateDNRRulesForExceptions();
			await store_default.set(Options, { filtersUpdatedAt: Date.now() });
			if (updated) await reloadMainEngine();
		}
	} finally {
		updating = false;
	}
}
var UPDATE_ENGINES_DELAY = 3600 * 1e3;
var setup = asyncSetup("adblocker", [addListener(async function adblockerEngines(options, lastOptions) {
	const enabledEngines = getEnabledEngines(options);
	const lastEnabledEngines = lastOptions && getEnabledEngines(lastOptions);
	const enginesChanged = lastEnabledEngines && (enabledEngines.length !== lastEnabledEngines.length || enabledEngines.some((id, i) => id !== lastEnabledEngines[i]));
	if (!await init("main") || enginesChanged) await reloadMainEngine();
	if (enginesChanged || options.filtersUpdatedAt < Date.now() - 36e5) await updateEngines();
})]);
//#endregion
export { UPDATE_ENGINES_DELAY, reloadMainEngine, setup, updateEngines };
