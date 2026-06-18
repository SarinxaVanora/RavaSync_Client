import store_default from "../npm/hybrids/src/store.js";
import ManagedConfig from "../store/managed-config.js";
import Options from "../store/options.js";
import { addListener } from "../utils/options-observer.js";
import { ACTION_PAUSE_ASSISTANT } from "../npm/@ghostery/config/dist/esm/actions.js";
import "../npm/@ghostery/config/dist/esm/flags.js";
import Config from "../store/config.js";
import { parseWithCache } from "../utils/request.js";
import { openNotification } from "./notifications.js";
//#region src/background/pause-assistant.js
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
async function updatePausedDomains(config, lastConfig) {
	if ((await store_default.resolve(ManagedConfig)).disableUserControl) return;
	const options = await store_default.resolve(Options);
	let paused = {};
	if (options.mode !== "default" || !options.pauseAssistant || !config.hasFlag("pause-assistant")) {
		for (const [domain, { assist }] of Object.entries(options.paused)) if (assist) paused[domain] = null;
	} else {
		for (const [domain, { actions, dismiss }] of Object.entries(config.domains)) if (!options.paused[domain] && !dismiss["pause-assistant"] && actions.includes("pause-assistant")) paused[domain] = {
			revokeAt: 0,
			assist: true
		};
		if (lastConfig) {
			for (const id of Object.keys(lastConfig.domains)) if (!config.hasAction(id, "pause-assistant") && lastConfig.hasAction(id, "pause-assistant") && !lastConfig.isDismissed(id, "pause-assistant")) paused[id] = null;
		}
	}
	const keys = Object.keys(paused);
	if (keys.length) {
		store_default.set(Options, { paused });
		console.log("[pause-assistant] Updating domains:", keys.map((k) => `${k} ${paused[k] ? "(add)" : "(remove)"}`).join(", "));
	}
}
store_default.observe(Config, async (_, config, lastConfig) => {
	updatePausedDomains(config, lastConfig);
});
addListener(async function pauseAssistant(options, lastOptions) {
	if (lastOptions && (options.pauseAssistant !== lastOptions.pauseAssistant || options.mode !== lastOptions.mode)) updatePausedDomains(await store_default.resolve(Config));
});
chrome.webNavigation.onCompleted.addListener(async (details) => {
	if (details.frameId === 0) {
		if ((await store_default.resolve(ManagedConfig)).disableUserControl) return;
		const options = await store_default.resolve(Options);
		if (options.mode !== "default" || !options.pauseAssistant) return;
		const config = await store_default.resolve(Config);
		if (!config.hasFlag("pause-assistant")) return;
		const hostname = parseWithCache(details.url).hostname;
		if (!hostname) return;
		const hasAction = config.hasAction(hostname, ACTION_PAUSE_ASSISTANT);
		if (hasAction && !config.isDismissed(hostname, "pause-assistant")) openNotification({
			id: "pause-assistant",
			tabId: details.tabId,
			position: "center",
			params: { hostname }
		});
		else {
			const options = await store_default.resolve(Options);
			const domain = Object.keys(options.paused).find((d) => hostname.endsWith(d));
			if (!hasAction && options.paused[domain]?.assist) openNotification({
				id: "pause-resume",
				tabId: details.tabId,
				params: { domain },
				position: "center"
			});
		}
	}
});
//#endregion
