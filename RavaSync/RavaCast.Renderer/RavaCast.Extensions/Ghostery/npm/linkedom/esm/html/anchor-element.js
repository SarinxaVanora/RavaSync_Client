import { stringAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/anchor-element.js
var tagName = "a";
/**
* @implements globalThis.HTMLAnchorElement
*/
var HTMLAnchorElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */ get href() {
		return encodeURI(decodeURI(stringAttribute.get(this, "href"))).trim();
	}
	set href(value) {
		stringAttribute.set(this, "href", decodeURI(value));
	}
	get download() {
		return encodeURI(decodeURI(stringAttribute.get(this, "download")));
	}
	set download(value) {
		stringAttribute.set(this, "download", decodeURI(value));
	}
	get target() {
		return stringAttribute.get(this, "target");
	}
	set target(value) {
		stringAttribute.set(this, "target", value);
	}
	get type() {
		return stringAttribute.get(this, "type");
	}
	set type(value) {
		stringAttribute.set(this, "type", value);
	}
	get rel() {
		return stringAttribute.get(this, "rel");
	}
	set rel(value) {
		stringAttribute.set(this, "rel", value);
	}
};
registerHTMLClass(tagName, HTMLAnchorElement);
//#endregion
export { HTMLAnchorElement };
