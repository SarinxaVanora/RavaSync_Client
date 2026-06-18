import store_default from "../npm/hybrids/src/store.js";
import { isOpera, isWebkit } from "../utils/browser-info.js";
//#region src/store/managed-config.js
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
var TRUSTED_DOMAINS_NONE_ID = "<none>";
var ManagedConfig = {
	disableOnboarding: false,
	disableUserControl: false,
	disableUserAccount: false,
	disableTrackersPreview: false,
	trustedDomains: [TRUSTED_DOMAINS_NONE_ID],
	customFilters: {
		enabled: false,
		filters: [String]
	},
	disableNotifications: (config) => config.disableOnboarding || config.disableUserControl,
	disableModes: (config) => config.disableOnboarding || config.disableUserControl || config.trustedDomains[0] !== "<none>" || config.customFilters.enabled,
	[store_default.connect]: async () => {
		if (isOpera() || isWebkit()) return {};
		try {
			let managedConfig = await chrome.storage.managed.get().catch((e) => {
				throw e;
			});
			if (Object.keys(managedConfig).length) chrome.storage.local.set({ managedConfig });
			else {
				const { managedConfig: fallbackConfig = {} } = await chrome.storage.local.get("managedConfig");
				managedConfig = fallbackConfig;
			}
			if (managedConfig.customFilters) managedConfig.customFilters = {
				enabled: true,
				filters: managedConfig.customFilters
			};
			return managedConfig || {};
		} catch {
			return {};
		}
	}
};
//#endregion
export { TRUSTED_DOMAINS_NONE_ID, ManagedConfig as default };
