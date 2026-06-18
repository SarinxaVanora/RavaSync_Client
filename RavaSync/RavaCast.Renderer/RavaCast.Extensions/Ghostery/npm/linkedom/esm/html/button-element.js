import { booleanAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/button-element.js
var tagName = "button";
/**
* @implements globalThis.HTMLButtonElement
*/
var HTMLButtonElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */
	get disabled() {
		return booleanAttribute.get(this, "disabled");
	}
	set disabled(value) {
		booleanAttribute.set(this, "disabled", value);
	}
	get name() {
		return this.getAttribute("name");
	}
	set name(value) {
		this.setAttribute("name", value);
	}
	get type() {
		return this.getAttribute("type");
	}
	set type(value) {
		this.setAttribute("type", value);
	}
};
registerHTMLClass(tagName, HTMLButtonElement);
//#endregion
export { HTMLButtonElement };
