import { isEdge, isSafari } from "./browser-info.js";
//#region src/utils/preprocessors.js
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
function checkUserAgent(pattern) {
	return navigator.userAgent.indexOf(pattern) !== -1;
}
/**
* Environment variables for preprocessor evaluation. These define which
* environment identifiers the extension supports and their current values.
* This map is the source of truth for all supported preprocessor envs.
*/
var ENV = new Map([
	["ext_ghostery", true],
	["env_firefox", false],
	["env_chromium", true],
	["env_edge", isEdge()],
	["env_safari", isSafari()],
	["env_mobile", checkUserAgent("Mobile")],
	["ext_ublock", true],
	["env_mv3", true],
	["ext_ubol", true],
	["cap_html_filtering", false],
	["cap_replace_modifier", false],
	["cap_user_stylesheet", true]
]);
//#endregion
export { ENV };
