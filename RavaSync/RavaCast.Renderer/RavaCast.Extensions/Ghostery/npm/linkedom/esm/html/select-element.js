import { booleanAttribute } from "../shared/attributes.js";
import { NodeList } from "../interface/node-list.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/select-element.js
var tagName = "select";
/**
* @implements globalThis.HTMLSelectElement
*/
var HTMLSelectElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	get options() {
		let children = new NodeList();
		let { firstElementChild } = this;
		while (firstElementChild) {
			if (firstElementChild.tagName === "OPTGROUP") children.push(...firstElementChild.children);
			else children.push(firstElementChild);
			firstElementChild = firstElementChild.nextElementSibling;
		}
		return children;
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
	/* c8 ignore stop */
	get value() {
		return this.querySelector("option[selected]")?.value;
	}
};
registerHTMLClass(tagName, HTMLSelectElement);
//#endregion
export { HTMLSelectElement };
