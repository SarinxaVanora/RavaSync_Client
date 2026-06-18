import { TooBigMsgError } from "./errors.js";
import { deflate } from "./zlib.js";
//#region node_modules/@whotracksme/reporting/communication/src/padding.js
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
function nextPow2(_v) {
	let v = _v | 0;
	v -= 1;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v += 1;
	return v;
}
function encodeLength(length) {
	if (length >= 32767) throw new TooBigMsgError("Message is too big");
	return 32768 | length;
}
function encodeWithPadding(message) {
	const compressed = deflate(message);
	const paddedSize = Math.max(1024, nextPow2(2 + compressed.length));
	const data = new Uint8Array(paddedSize);
	new DataView(data.buffer).setUint16(0, encodeLength(compressed.length));
	data.set(compressed, 2);
	return data;
}
//#endregion
export { encodeWithPadding };
