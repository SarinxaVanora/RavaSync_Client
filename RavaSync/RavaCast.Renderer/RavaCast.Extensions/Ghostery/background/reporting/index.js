import { isFirefox } from "../../utils/browser-info.js";
import { addListener } from "../../utils/options-observer.js";
import { describeLoggers, setLogLevel } from "../../npm/@whotracksme/reporting/reporting/src/logger.js";
import "../../npm/@whotracksme/reporting/reporting/src/index.js";
import config_default from "./config.js";
import communication_default from "./communication.js";
import url_reporter_default from "./url-reporter.js";
import webrequest_reporter_default from "./webrequest-reporter.js";
//#region src/background/reporting/index.js
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
(async () => {
	try {
		const key = "ghosteryReportingLoggerConfig";
		const { [key]: config } = await chrome.storage.local.get(key) || {};
		if (config) for (const { level, prefix = "*" } of config) setLogLevel(level, { prefix });
		else setLogLevel("off");
	} catch (e) {
		console.warn("Failed to apply logger overwrites", e);
	}
})();
addListener("terms", async function reporting(terms) {
	if (terms && !isFirefox()) {
		if (webrequest_reporter_default) webrequest_reporter_default.init().catch((e) => {
			console.warn("Failed to initialize request reporting. Leaving the module disabled and continue.", e);
		});
		url_reporter_default.init().catch((e) => {
			console.warn("Failed to initialize urlReporting. Leaving the module disabled and continue.", e);
		});
	} else {
		try {
			url_reporter_default.unload();
		} catch (e) {
			console.error(e);
		}
		try {
			webrequest_reporter_default?.unload();
		} catch (e) {
			console.error(e);
		}
	}
});
(globalThis.ghostery ??= {}).WTM = {
	communication: communication_default,
	urlReporter: url_reporter_default,
	config: config_default,
	webRequestReporter: webrequest_reporter_default,
	extensionStartedAt: /* @__PURE__ */ new Date(),
	logging: {
		setLogLevel,
		describeLoggers
	}
};
//#endregion
