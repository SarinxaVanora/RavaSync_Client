import LRU from "./LRU.js";
//#region node_modules/@whotracksme/reporting/reporting/src/fixed-size-cache.js
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
var FixedSizeCache = class {
	constructor(buildValue, size, buildKey) {
		this._buildValue = buildValue;
		this._buildKey = buildKey;
		this._maxKeySize = 1e3;
		this._hitCounter = 0;
		this._missCounter = 0;
		this.lru = new LRU(size);
	}
	get(argument) {
		const key = this._buildKey ? this._buildKey(argument) : argument;
		let value = this.lru.get(key);
		if (value !== void 0) {
			this._hitCounter += 1;
			return value;
		}
		this._missCounter += 1;
		value = this._buildValue(argument);
		if (!key || key.length > this._maxKeySize) return value;
		this.lru.set(key, value);
		return value;
	}
};
//#endregion
export { FixedSizeCache as default };
