import { stringAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/source-element.js
var tagName = "source";
/**
* @implements globalThis.HTMLSourceElement
*/
var HTMLSourceElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */
	get src() {
		return stringAttribute.get(this, "src");
	}
	set src(value) {
		stringAttribute.set(this, "src", value);
	}
	get srcset() {
		return stringAttribute.get(this, "srcset");
	}
	set srcset(value) {
		stringAttribute.set(this, "srcset", value);
	}
	get sizes() {
		return stringAttribute.get(this, "sizes");
	}
	set sizes(value) {
		stringAttribute.set(this, "sizes", value);
	}
	get type() {
		return stringAttribute.get(this, "type");
	}
	set type(value) {
		stringAttribute.set(this, "type", value);
	}
};
registerHTMLClass(tagName, HTMLSourceElement);
//#endregion
export { HTMLSourceElement };
