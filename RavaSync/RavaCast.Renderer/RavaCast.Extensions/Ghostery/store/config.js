import store_default from "../npm/hybrids/src/store.js";
//#region src/store/config.js
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
var Config = {
	enabled: true,
	updatedAt: 0,
	domains: store_default.record({
		actions: [String],
		issueUrl: "",
		dismiss: store_default.record(false)
	}),
	flags: store_default.record({
		percentage: 0,
		enabled: false
	}),
	hasAction({ domains, enabled }) {
		const hostnames = /* @__PURE__ */ new Map();
		return (hostname, action) => {
			if (!enabled || !hostname) return;
			let actions = hostnames.get(hostname);
			if (!actions) {
				actions = /* @__PURE__ */ new Map();
				hostnames.set(hostname, actions);
			}
			if (!actions.has(action)) {
				const domain = Object.keys(domains).find((d) => hostname.endsWith(d));
				const value = !!domain && domains[domain].actions.includes(action);
				actions.set(action, value);
				return value;
			}
			return actions.get(action);
		};
	},
	isDismissed({ domains, enabled }) {
		return (hostname, action) => {
			if (!enabled || !hostname) return;
			const domain = Object.keys(domains).find((d) => hostname.endsWith(d));
			if (!domain) return false;
			return !!domains[domain].dismiss[action];
		};
	},
	hasFlag({ flags, enabled }) {
		return (flag) => {
			if (!enabled || !flag || !flags[flag]) return false;
			return flags[flag].enabled;
		};
	},
	[store_default.connect]: {
		async get() {
			const { config = {} } = await chrome.storage.local.get(["config"]);
			return config;
		},
		async set(_, values) {
			values ||= {};
			await chrome.storage.local.set({ config: values });
			return values;
		}
	}
};
chrome.storage.onChanged.addListener((changes) => {
	if (changes["config"]) store_default.clear(Config, false);
});
async function dismissAction(domain, action) {
	const config = await store_default.resolve(Config);
	const id = Object.keys(config.domains).find((d) => domain.endsWith(d));
	await store_default.set(Config, { domains: { [id]: { dismiss: { [action]: true } } } });
}
function resolveFlag(id) {
	const promise = new Promise((resolve) => {
		store_default.resolve(Config).then((config) => {
			const value = config.hasFlag(id);
			promise.enabled = value;
			resolve(value);
		}, () => resolve(false));
	});
	promise.enabled = false;
	return promise;
}
//#endregion
export { Config as default, dismissAction, resolveFlag };
