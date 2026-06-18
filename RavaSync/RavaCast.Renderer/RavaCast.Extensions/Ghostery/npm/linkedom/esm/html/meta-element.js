import { stringAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/meta-element.js
var tagName = "meta";
/**
* @implements globalThis.HTMLMetaElement
*/
var HTMLMetaElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */
	get name() {
		return stringAttribute.get(this, "name");
	}
	set name(value) {
		stringAttribute.set(this, "name", value);
	}
	get httpEquiv() {
		return stringAttribute.get(this, "http-equiv");
	}
	set httpEquiv(value) {
		stringAttribute.set(this, "http-equiv", value);
	}
	get content() {
		return stringAttribute.get(this, "content");
	}
	set content(value) {
		stringAttribute.set(this, "content", value);
	}
	get charset() {
		return stringAttribute.get(this, "charset");
	}
	set charset(value) {
		stringAttribute.set(this, "charset", value);
	}
	get media() {
		return stringAttribute.get(this, "media");
	}
	set media(value) {
		stringAttribute.set(this, "media", value);
	}
};
registerHTMLClass(tagName, HTMLMetaElement);
//#endregion
export { HTMLMetaElement };
