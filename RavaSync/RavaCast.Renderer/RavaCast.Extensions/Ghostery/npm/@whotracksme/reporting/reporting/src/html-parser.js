import { DOMParser } from "../../../../linkedom/esm/dom/parser.js";
import "../../../../linkedom/esm/cached.js";
//#region node_modules/@whotracksme/reporting/reporting/src/html-parser.js
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
function parseHtml(html) {
	return new DOMParser().parseFromString(html, "text/html");
}
//#endregion
export { parseHtml as default };
