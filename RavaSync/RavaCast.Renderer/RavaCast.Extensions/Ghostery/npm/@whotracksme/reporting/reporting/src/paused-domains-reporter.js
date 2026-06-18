import logger_default from "./logger.js";
import { requireParam } from "./utils.js";
//#region node_modules/@whotracksme/reporting/reporting/src/paused-domains-reporter.js
/**
* WhoTracks.Me
* https://whotracks.me/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var PausedDomainsReporter = class {
	constructor({ filterModeProvider }) {
		this.active = false;
		this.filterModeProvider = requireParam(filterModeProvider);
	}
	async init() {
		this.active = true;
	}
	unload() {
		this.active = false;
	}
	onPauseEvent({ hostname, paused }) {
		if (!this.active) return;
		logger_default.info(hostname, "is now", paused ? "paused" : "unpaused");
		const event = {
			filterMode: this.filterModeProvider(),
			hostname,
			paused,
			ts: Date.now()
		};
		this._processEvent(event).catch((e) => {
			logger_default.error("Failed to process event", event, e);
		});
	}
	async _processEvent(event) {
		logger_default.warn("[STUB] handing of pause/unpause is not yet implemented", event);
	}
};
//#endregion
export { PausedDomainsReporter as default };
