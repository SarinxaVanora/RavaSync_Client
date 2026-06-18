import { CHANGED, VALUE } from "../shared/symbols.js";
import { $String, ignoreCase } from "../shared/utils.js";
import { attrAsJSON } from "../shared/jsdon.js";
import { attributeChangedCallback } from "./custom-element-registry.js";
import { attributeChangedCallback as attributeChangedCallback$1 } from "./mutation-observer.js";
import { emptyAttributes } from "../shared/attributes.js";
import { Node } from "./node.js";
import { escape } from "../shared/text-escaper.js";
//#region node_modules/linkedom/esm/interface/attr.js
var QUOTE = /"/g;
/**
* @implements globalThis.Attr
*/
var Attr = class Attr extends Node {
	constructor(ownerDocument, name, value = "") {
		super(ownerDocument, name, 2);
		this.ownerElement = null;
		this.name = $String(name);
		this[VALUE] = $String(value);
		this[CHANGED] = false;
	}
	get value() {
		return this[VALUE];
	}
	set value(newValue) {
		const { [VALUE]: oldValue, name, ownerElement } = this;
		this[VALUE] = $String(newValue);
		this[CHANGED] = true;
		if (ownerElement) {
			attributeChangedCallback$1(ownerElement, name, oldValue);
			attributeChangedCallback(ownerElement, name, oldValue, this[VALUE]);
		}
	}
	cloneNode() {
		const { ownerDocument, name, [VALUE]: value } = this;
		return new Attr(ownerDocument, name, value);
	}
	toString() {
		const { name, [VALUE]: value } = this;
		if (emptyAttributes.has(name) && !value) return ignoreCase(this) ? name : `${name}=""`;
		return `${name}="${(ignoreCase(this) ? value : escape(value)).replace(QUOTE, "&quot;")}"`;
	}
	toJSON() {
		const json = [];
		attrAsJSON(this, json);
		return json;
	}
};
//#endregion
export { Attr };
