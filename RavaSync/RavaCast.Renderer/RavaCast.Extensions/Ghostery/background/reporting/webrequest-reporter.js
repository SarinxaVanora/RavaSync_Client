import store_default from "../../npm/hybrids/src/store.js";
import Options, { getPausedDetails } from "../../store/options.js";
import "../../npm/@ghostery/config/dist/esm/actions.js";
import Config from "../../store/config.js";
import ExtendedRequest from "../../utils/request.js";
import { updateTabStats } from "../stats.js";
import RequestReporter from "../../npm/@whotracksme/reporting/reporting/src/request/index.js";
import "../../npm/@whotracksme/reporting/reporting/src/index.js";
import config_default from "./config.js";
import communication_default from "./communication.js";
import url_reporter_default from "./url-reporter.js";
//#region src/background/reporting/webrequest-reporter.js
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
var webRequestReporter = null;
if (chrome.webRequest) {
	let remoteConfig;
	store_default.resolve(Config).then((remote) => {
		remoteConfig = remote;
	});
	try {
		webRequestReporter = new RequestReporter(config_default.request, {
			onMessageReady: url_reporter_default.forwardRequestReporterMessage.bind(url_reporter_default),
			countryProvider: url_reporter_default.countryProvider,
			trustedClock: communication_default.trustedClock,
			isRequestAllowed: (state) => {
				const options = store_default.get(Options);
				const hostname = state.tabUrlParts.hostname;
				return !options.blockTrackers || !!getPausedDetails(options, hostname) || remoteConfig?.hasAction(hostname, "disable-antitracking-modification");
			},
			onTrackerInteraction: (event, state) => {
				if (event === "observed") return;
				const request = ExtendedRequest.fromRequestDetails({
					url: state.url,
					originUrl: state.tabUrl
				});
				request.modified = true;
				updateTabStats(state.tabId, [request]);
			}
		});
		chrome.runtime.onMessage.addListener((msg, sender) => {
			if (msg.action === "mousedown") webRequestReporter.recordClick(msg.event, msg.context, msg.href, sender);
		});
	} catch (e) {
		console.warn("Failed to create webRequestReporter. Leaving it disabled.", e);
	}
}
var webrequest_reporter_default = webRequestReporter;
//#endregion
export { webrequest_reporter_default as default };
