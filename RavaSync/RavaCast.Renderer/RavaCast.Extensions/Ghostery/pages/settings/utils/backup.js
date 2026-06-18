import store_default from "../../../npm/hybrids/src/store.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import CustomFilters from "../../../store/custom-filters.js";
import Options, { SYNC_OPTIONS } from "../../../store/options.js";
import { download } from "../../../utils/files.js";
import ElementPickerSelectors from "../../../store/element-picker-selectors.js";
//#region src/pages/settings/utils/backup.js
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
var DATA_VERSION = 1;
async function exportToFile() {
	const options = await store_default.resolve(Options);
	const customFilters = await store_default.resolve(CustomFilters);
	const elementPickerSelectors = await store_default.resolve(ElementPickerSelectors);
	const cleanedOptions = Object.fromEntries(SYNC_OPTIONS.map((key) => [key, options[key]]));
	cleanedOptions.paused = Object.entries(cleanedOptions.paused).reduce((acc, [domain, details]) => {
		if (!details.managed && !details.assist && !details.revokeAt) acc[domain] = { revokeAt: 0 };
		return acc;
	}, {});
	const data = {
		timestamp: Date.now(),
		version: DATA_VERSION,
		options: cleanedOptions
	};
	if (customFilters.text) data.customFilters = customFilters.text;
	if (Object.keys(elementPickerSelectors.hostnames).length > 0) data.blockedElements = elementPickerSelectors.hostnames;
	await download({
		data: JSON.stringify(data, null, 2),
		filename: `ghostery-settings-${(/* @__PURE__ */ new Date()).toISOString()}.json`
	});
}
function resolveBackupFormat(text) {
	const data = JSON.parse(text);
	if (!data || typeof data !== "object") throw new Error(msg`Error: invalid file format.`);
	return {
		timestamp: data.timestamp || data.timeStamp,
		version: typeof data.version === "number" ? data.version : DATA_VERSION,
		options: data.options || {
			theme: data.userSettings?.uiTheme || "",
			customFilters: {
				enabled: !!data.userFilters,
				trustedScriptlets: data.userSettings?.userFiltersTrusted
			},
			paused: data.whitelist?.reduce((acc, id) => {
				acc[id] = { revokeAt: 0 };
				return acc;
			}, {}) || {}
		},
		customFilters: data.customFilters || data.userFilters || "",
		blockedElements: data.blockedElements || null
	};
}
function importFromFile(event) {
	const input = event.target;
	if (!input.files || input.files.length === 0) return;
	const file = event.target.files[0];
	const reader = new FileReader();
	const result = new Promise((resolve, reject) => {
		reader.onload = async (e) => {
			input.value = "";
			try {
				const data = resolveBackupFormat(e.target.result);
				if (typeof data.timestamp !== "number") throw new Error(msg`Error: invalid file format.`);
				if (data.version !== DATA_VERSION) throw new Error(msg`Error: unsupported file version ${data.version} - expected ${DATA_VERSION}.`);
				await store_default.set(CustomFilters, { text: data.customFilters });
				await store_default.set(ElementPickerSelectors, { hostnames: data.blockedElements });
				const options = await store_default.resolve(Options);
				await store_default.set(Options, {
					paused: Object.fromEntries(Object.entries(options.paused).filter(([, details]) => !details.managed && !details.assist && !details.revokeAt)),
					exceptions: null
				});
				const cleanedOptions = Object.fromEntries(SYNC_OPTIONS.map((key) => [key, data.options[key]]).filter(([, value]) => value !== void 0));
				await store_default.set(Options, cleanedOptions);
				resolve(msg`The backup from ${new Date(data.timestamp).toLocaleString()} imported successfully.`);
			} catch (error) {
				reject(error);
			}
		};
	});
	reader.readAsText(file);
	return result;
}
//#endregion
export { exportToFile, importFromFile };
