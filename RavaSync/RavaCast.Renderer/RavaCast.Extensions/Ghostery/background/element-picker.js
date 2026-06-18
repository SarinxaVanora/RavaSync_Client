import store_default from "../npm/hybrids/src/store.js";
import { parseFilters } from "../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../npm/@ghostery/adblocker/dist/esm/index.js";
import { ELEMENT_PICKER_ENGINE, create, init, remove } from "../utils/engines.js";
import ElementPickerSelectors from "../store/element-picker-selectors.js";
import { reloadMainEngine, setup } from "./adblocker/engines.js";
//#region src/background/element-picker.js
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
async function openElementPicker(tabId) {
	await chrome.scripting.executeScript({
		injectImmediately: true,
		world: chrome.scripting.ExecutionWorld?.ISOLATED ?? "ISOLATED",
		target: { tabId },
		files: ["/content_scripts/element-picker.js"]
	});
}
store_default.observe(ElementPickerSelectors, async (_, model, lastModel) => {
	let entries = Object.entries(model.hostnames);
	if (!lastModel) {
		if (!entries.length) return;
		if (await init("element-picker-selectors")) return;
	}
	if (entries.length) {
		const elementPickerFilters = entries.reduce((acc, [hostname, selectors]) => {
			for (const selector of selectors) acc.push(`${hostname}##${selector}`);
			return acc;
		}, []);
		const { cosmeticFilters } = parseFilters(elementPickerFilters.join("\n"));
		await create(ELEMENT_PICKER_ENGINE, { cosmeticFilters });
		console.log(`[element-picker] Engine updated with ${elementPickerFilters.length} selectors for ${entries.length} hostnames`);
	} else {
		remove(ELEMENT_PICKER_ENGINE);
		console.log("[element-picker] No selectors - engine removed");
	}
	setup.pending && await setup.pending;
	await reloadMainEngine();
});
store_default.resolve(ElementPickerSelectors);
//#endregion
export { openElementPicker };
