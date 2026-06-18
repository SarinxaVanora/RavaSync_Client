import store_default from "../../npm/hybrids/src/store.js";
import Options from "../../store/options.js";
import { addListener } from "../../utils/options-observer.js";
import Config from "../../store/config.js";
import asyncSetup from "../../utils/setup.js";
import DailyStats from "../../store/daily-stats.js";
import { getStorage, saveStorage } from "../../utils/telemetry.js";
import Metrics from "./metrics.js";
import detectAttribution from "./attribution.js";
//#region src/background/telemetry/index.js
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
var runner;
var setup = asyncSetup("telemetry", [(async () => {
	const { version } = chrome.runtime.getManifest();
	const metrics = await getStorage();
	if (!metrics.installDate) {
		metrics.installDate = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
		await saveStorage(metrics);
	}
	runner = new Metrics({
		METRICS_BASE_URL: "https://d.ghostery.com",
		EXTENSION_VERSION: version,
		storage: metrics,
		saveStorage,
		getConf: async () => {
			const yesterdayId = (/* @__PURE__ */ new Date(Date.now() - 864e5)).toISOString().split("T")[0];
			const [options, config, dailyStats] = await Promise.all([
				store_default.resolve(Options),
				store_default.resolve(Config),
				store_default.resolve(DailyStats, yesterdayId)
			]);
			return {
				options,
				config,
				yesterdayPages: dailyStats.pages,
				userSettings: await chrome.action?.getUserSettings?.(),
				isAllowedIncognitoAccess: await chrome.extension.isAllowedIncognitoAccess()
			};
		},
		log: console.debug.bind(console, "[telemetry]")
	});
})()]);
var enabled = false;
addListener(async function telemetry({ terms, feedback }, lastOptions) {
	enabled = terms && feedback;
	if (lastOptions && lastOptions.terms) return;
	if (terms) {
		setup.pending && await setup.pending;
		if (runner.isJustInstalled()) {
			await runner.setUTMs(await detectAttribution());
			runner.ping("install");
		}
		runner.setUninstallUrl();
		if (feedback) runner.ping("active");
	} else chrome.runtime.setUninstallURL("https://mygho.st/fresh-uninstalls");
});
async function recordSerpVisit() {
	setup.pending && await setup.pending;
	if (!runner) return;
	await runner.recordSerpVisit();
}
chrome.runtime.onMessage.addListener((msg) => {
	if (enabled && msg.action.startsWith("telemetry:")) (async () => {
		setup.pending && await setup.pending;
		switch (msg.action) {
			case "telemetry:ping":
				await runner.ping(msg.event);
				break;
			case "telemetry:modeTouched":
				runner.storage.modeTouched = true;
				await saveStorage(runner.storage);
				console.debug("[telemetry] \"modeTouched\" flag set");
				break;
		}
	})();
});
//#endregion
export { recordSerpVisit };
