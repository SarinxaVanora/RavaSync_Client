import { HashProb } from "./hash-detector-v2/index.js";
//#region node_modules/@whotracksme/reporting/reporting/src/hash-detector-v2.js
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
var classifier = new HashProb();
function isHash(str) {
	return classifier.isHash(str);
}
//#endregion
export { isHash };
