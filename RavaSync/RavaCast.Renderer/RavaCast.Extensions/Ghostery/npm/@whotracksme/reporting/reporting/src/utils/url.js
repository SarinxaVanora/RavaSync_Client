import immutable_url_default from "../../../../../@ghostery/url-parser/dist/esm/immutable-url.js";
import "../../../../../@ghostery/url-parser/dist/esm/index.js";
//#region node_modules/@whotracksme/reporting/reporting/src/utils/url.js
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
* This is an abstraction over URL with caching and basic error handling built in. The main
* difference is that this catches exceptions from the URL constructor (when the url is invalid)
* and returns null instead in these cases.
* @param String url
* @returns {URL} parsed URL if valid is parseable, otherwise null;
*/
function parse(url) {
	if (typeof url !== "string") return null;
	try {
		return new immutable_url_default(url);
	} catch (e) {
		return null;
	}
}
function tryDecode(fn) {
	return (url) => {
		if (typeof url !== "string") return url;
		if (url.indexOf("%") === -1) return url;
		try {
			return fn(url);
		} catch (e) {
			return url;
		}
	};
}
var tryDecodeURIComponent = tryDecode(decodeURIComponent);
//#endregion
export { parse, tryDecodeURIComponent };
