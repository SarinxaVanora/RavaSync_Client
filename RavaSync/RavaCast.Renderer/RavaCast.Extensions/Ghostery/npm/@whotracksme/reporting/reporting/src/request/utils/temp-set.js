//#region node_modules/@whotracksme/reporting/reporting/src/request/utils/temp-set.js
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
/** Set like class whose members are removed after a specific amount of time
*/
var TempSet = class {
	constructor() {
		this._items = /* @__PURE__ */ new Set();
		this._timeouts = /* @__PURE__ */ new Set();
	}
	contains(item) {
		return this._items.has(item);
	}
	has(item) {
		return this.contains(item);
	}
	add(item, ttl) {
		this._items.add(item);
		const timeout = setTimeout(function() {
			this.delete(item);
			this._timeouts.delete(timeout);
		}.bind(this), ttl || 0);
		this._timeouts.add(timeout);
	}
	delete(item) {
		this._items.delete(item);
	}
	clear() {
		for (const t of this._timeouts) clearTimeout(t);
		this._timeouts.clear();
		this._items.clear();
	}
};
//#endregion
export { TempSet as default };
