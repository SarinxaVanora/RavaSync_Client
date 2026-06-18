import store_default from "../../../npm/hybrids/src/store.js";
import { parse } from "../../../npm/tldts-experimental/dist/es6/index.js";
import { getTrackerByURL } from "../../../utils/trackerdb.js";
import Tracker from "../../../store/tracker.js";
//#region src/pages/redirect-protection/store/target.js
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
var Target = {
	url: "",
	hostname: "",
	tracker: Tracker,
	[store_default.connect]: async () => {
		let url = "";
		url = (await chrome.runtime.sendMessage({ action: "getRedirectUrl" })).url;
		return {
			url,
			hostname: url && parse(url).hostname,
			tracker: url && await getTrackerByURL(url)
		};
	}
};
//#endregion
export { Target as default };
