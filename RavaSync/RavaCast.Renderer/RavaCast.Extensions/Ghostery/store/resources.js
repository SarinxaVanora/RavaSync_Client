import store_default from "../npm/hybrids/src/store.js";
//#region src/store/resources.js
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
var Resources = {
	checksums: store_default.record(""),
	autoconsent: store_default.record(0),
	[store_default.connect]: {
		get: async () => chrome.storage.local.get("resources").then(({ resources = {} }) => resources),
		set: async (_, values) => {
			await chrome.storage.local.set({ resources: values });
			return values;
		}
	}
};
//#endregion
export { Resources as default };
