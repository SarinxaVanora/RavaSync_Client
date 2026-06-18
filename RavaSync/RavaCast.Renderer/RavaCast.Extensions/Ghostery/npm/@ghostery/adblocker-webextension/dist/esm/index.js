import "../../../../tldts-experimental/dist/es6/index.js";
import "../../../adblocker/dist/esm/config.js";
import "../../../adblocker/dist/esm/fetch.js";
import "../../../adblocker/dist/esm/utils.js";
import "../../../adblocker/dist/esm/request.js";
import "../../../adblocker/dist/esm/filters/cosmetic.js";
import "../../../adblocker/dist/esm/filters/network.js";
import "../../../adblocker/dist/esm/preprocessor.js";
import "../../../adblocker/dist/esm/lists.js";
import "../../../adblocker/dist/esm/resources.js";
import "../../../adblocker/dist/esm/engine/reverse-index.js";
import "../../../adblocker/dist/esm/engine/engine.js";
import "../../../adblocker/dist/esm/index.js";
//#region node_modules/@ghostery/adblocker-webextension/dist/esm/index.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function isFirefox() {
	try {
		return navigator.userAgent.indexOf("Firefox") !== -1;
	} catch (e) {
		return false;
	}
}
/**
* There are different ways to inject scriptlets ("push" vs "pull").
* This function should decide based on the environment what to use:
*
* 1) "Pushing" means the adblocker will listen on "onCommitted" events
*    and then execute scripts by running the tabs.executeScript API.
* 2) "Pulling" means the adblocker will inject a content script, which
*    runs before the page loads (and on the DOM changes), fetches
*    scriplets from the background and runs them.
*
* Note:
* - the "push" model requires permission to the webNavigation API.
*   If that is not available, the implementation will fall back to the
*   "pull" model, which does not have this requirement.
*/
function usePushScriptsInjection() {
	return !isFirefox();
}
usePushScriptsInjection();
new TextEncoder();
//#endregion
