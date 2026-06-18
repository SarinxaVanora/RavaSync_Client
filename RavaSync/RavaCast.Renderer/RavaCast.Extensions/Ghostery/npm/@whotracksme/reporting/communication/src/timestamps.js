//#region node_modules/@whotracksme/reporting/communication/src/timestamps.js
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
/**
* Contains functions to generate timestamps used in WhoTracksMe messages.
* As a general rule, all timestamps in the context of WhoTracksMe will be in UTC.
*
* To mitigate the risk of fingerprinting based on clock drift, messages
* should not include high resolution timestamps, but instead should be rounded.
*/
function getTrustedUtcTime() {
	return /* @__PURE__ */ new Date();
}
function getTimeAsYYYYMMDD(now) {
	return (now || getTrustedUtcTime()).toISOString().replace(/[^0-9]/g, "").slice(0, 8);
}
//#endregion
export { getTimeAsYYYYMMDD };
