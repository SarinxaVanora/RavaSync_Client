import { booleanAttribute, stringAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/link-element.js
var tagName = "link";
/**
* @implements globalThis.HTMLLinkElement
*/
var HTMLLinkElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */ get disabled() {
		return booleanAttribute.get(this, "disabled");
	}
	set disabled(value) {
		booleanAttribute.set(this, "disabled", value);
	}
	get href() {
		return stringAttribute.get(this, "href").trim();
	}
	set href(value) {
		stringAttribute.set(this, "href", value);
	}
	get hreflang() {
		return stringAttribute.get(this, "hreflang");
	}
	set hreflang(value) {
		stringAttribute.set(this, "hreflang", value);
	}
	get media() {
		return stringAttribute.get(this, "media");
	}
	set media(value) {
		stringAttribute.set(this, "media", value);
	}
	get rel() {
		return stringAttribute.get(this, "rel");
	}
	set rel(value) {
		stringAttribute.set(this, "rel", value);
	}
	get type() {
		return stringAttribute.get(this, "type");
	}
	set type(value) {
		stringAttribute.set(this, "type", value);
	}
};
registerHTMLClass(tagName, HTMLLinkElement);
//#endregion
export { HTMLLinkElement };
