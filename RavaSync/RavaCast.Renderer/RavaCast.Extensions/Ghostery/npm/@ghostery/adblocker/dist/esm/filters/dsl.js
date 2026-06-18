//#region node_modules/@ghostery/adblocker/dist/esm/filters/dsl.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var NetworkBuilder = class {
	constructor() {
		this.options = /* @__PURE__ */ new Set();
		this.prefix = void 0;
		this.infix = void 0;
		this.suffix = void 0;
		this.redirect = void 0;
	}
	blockRequestsWithType(t) {
		if (this.options.has(t)) throw new Error(`Already blocking type ${t}`);
		this.options.add(t);
		return this;
	}
	images() {
		return this.blockRequestsWithType("image");
	}
	scripts() {
		return this.blockRequestsWithType("script");
	}
	frames() {
		return this.blockRequestsWithType("frame");
	}
	fonts() {
		return this.blockRequestsWithType("font");
	}
	medias() {
		return this.blockRequestsWithType("media");
	}
	styles() {
		return this.blockRequestsWithType("css");
	}
	redirectTo(redirect) {
		if (this.redirect !== void 0) throw new Error(`Already redirecting: ${this.redirect}`);
		this.redirect = `redirect=${redirect}`;
		return this;
	}
	urlContains(infix) {
		if (this.infix !== void 0) throw new Error(`Already matching pattern: ${this.infix}`);
		this.infix = infix;
		return this;
	}
	urlStartsWith(prefix) {
		if (this.prefix !== void 0) throw new Error(`Already matching prefix: ${this.prefix}`);
		this.prefix = `|${prefix}`;
		return this;
	}
	urlEndsWith(suffix) {
		if (this.suffix !== void 0) throw new Error(`Already matching suffix: ${this.suffix}`);
		this.suffix = `${suffix}|`;
		return this;
	}
	withHostname(hostname) {
		if (this.prefix !== void 0) throw new Error(`Cannot match hostname if filter already has prefix: ${this.prefix}`);
		this.prefix = `||${hostname}^`;
		return this;
	}
	toString() {
		const parts = [];
		if (this.prefix !== void 0) parts.push(this.prefix);
		if (this.infix !== void 0) parts.push(this.infix);
		if (this.suffix !== void 0) parts.push(this.suffix);
		const options = ["important"];
		if (this.options.size !== 0) for (const option of this.options) options.push(option);
		if (this.redirect !== void 0) options.push(this.redirect);
		return `${parts.length === 0 ? "*" : parts.join("*")}$${options.join(",")}`;
	}
};
function block() {
	return new NetworkBuilder();
}
//#endregion
export { block };
