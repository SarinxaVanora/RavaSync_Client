import { booleanAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { TextElement } from "./text-element.js";
//#region node_modules/linkedom/esm/html/text-area-element.js
var tagName = "textarea";
/**
* @implements globalThis.HTMLTextAreaElement
*/
var HTMLTextAreaElement = class extends TextElement {
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
	get placeholder() {
		return this.getAttribute("placeholder");
	}
	set placeholder(value) {
		this.setAttribute("placeholder", value);
	}
	get type() {
		return this.getAttribute("type");
	}
	set type(value) {
		this.setAttribute("type", value);
	}
	get value() {
		return this.textContent;
	}
	set value(content) {
		this.textContent = content;
	}
};
registerHTMLClass(tagName, HTMLTextAreaElement);
//#endregion
export { HTMLTextAreaElement };
