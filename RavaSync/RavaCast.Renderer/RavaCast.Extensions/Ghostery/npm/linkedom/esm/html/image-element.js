import { numericAttribute, stringAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/image-element.js
var tagName = "img";
/**
* @implements globalThis.HTMLImageElement
*/
var HTMLImageElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */
	get alt() {
		return stringAttribute.get(this, "alt");
	}
	set alt(value) {
		stringAttribute.set(this, "alt", value);
	}
	get sizes() {
		return stringAttribute.get(this, "sizes");
	}
	set sizes(value) {
		stringAttribute.set(this, "sizes", value);
	}
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
	get title() {
		return stringAttribute.get(this, "title");
	}
	set title(value) {
		stringAttribute.set(this, "title", value);
	}
	get width() {
		return numericAttribute.get(this, "width");
	}
	set width(value) {
		numericAttribute.set(this, "width", value);
	}
	get height() {
		return numericAttribute.get(this, "height");
	}
	set height(value) {
		numericAttribute.set(this, "height", value);
	}
};
registerHTMLClass(tagName, HTMLImageElement);
//#endregion
export { HTMLImageElement };
