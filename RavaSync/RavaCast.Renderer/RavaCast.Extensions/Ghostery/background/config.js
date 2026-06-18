import store_default from "../npm/hybrids/src/store.js";
import { CDN_URL } from "../utils/urls.js";
import Config from "../store/config.js";
import { filter } from "../utils/config.js";
//#region src/background/config.js
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
var CONFIG_URL = CDN_URL + "configs/v1.json";
var HALF_HOUR_IN_MS = 1e3 * 60 * 30;
async function syncConfig() {
	const config = await store_default.resolve(Config);
	if (config.updatedAt > Date.now() - HALF_HOUR_IN_MS) return;
	try {
		const fetchedConfig = await fetch(CONFIG_URL).then((res) => {
			if (!res.ok) throw new Error("Failed to fetch the remote config");
			return res.json();
		});
		const domains = { ...config.domains };
		for (const name of Object.keys(domains)) if (fetchedConfig.domains[name] === void 0) domains[name] = null;
		for (const [name, item] of Object.entries(fetchedConfig.domains)) domains[name] = filter(item) ? item : null;
		const flags = { ...config.flags };
		for (const name of Object.keys(flags)) if (fetchedConfig.flags[name] === void 0) flags[name] = null;
		for (const [name, items] of Object.entries(fetchedConfig.flags)) {
			const item = items.find((item) => filter(item));
			if (!item) {
				flags[name] = null;
				continue;
			}
			const percentage = flags[name]?.percentage ?? Math.floor(Math.random() * 100) + 1;
			flags[name] = {
				percentage,
				enabled: percentage <= item.percentage
			};
		}
		await store_default.set(Config, {
			domains,
			flags,
			updatedAt: Date.now()
		});
		console.log("[config] Remote config synced");
	} catch (e) {
		console.error("[config] Failed to sync remote config:", e);
	}
}
function waitForConfigSync(timeout = 5e3) {
	return new Promise((resolve) => {
		store_default.resolve(Config).then((config) => {
			if (config.updatedAt > 0) {
				resolve();
				return;
			}
			let unobserve;
			const timer = setTimeout(() => {
				unobserve();
				resolve();
			}, timeout);
			unobserve = store_default.observe(Config, (_, config) => {
				if (config.updatedAt > 0) {
					clearTimeout(timer);
					unobserve();
					resolve();
				}
			});
		});
	});
}
store_default.observe(Config, (_, config, lastConfig) => {
	if (!lastConfig || config.updatedAt === 0) syncConfig();
});
store_default.resolve(Config);
//#endregion
export { waitForConfigSync };
