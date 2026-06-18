import store_default from "../npm/hybrids/src/store.js";
//#region src/store/element-picker-selectors.js
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
var ElementPickerSelectors = {
	hostnames: store_default.record([String]),
	[store_default.connect]: {
		get: async () => {
			const { elementPickerSelectors = {} } = await chrome.storage.local.get(["elementPickerSelectors"]);
			return elementPickerSelectors;
		},
		set: async (id, values) => {
			await chrome.storage.local.set({ elementPickerSelectors: values });
			return values;
		}
	}
};
chrome.storage.local.onChanged.addListener((changes) => {
	if (changes.elementPickerSelectors) store_default.sync(ElementPickerSelectors, changes.elementPickerSelectors.newValue);
});
//#endregion
export { ElementPickerSelectors as default };
