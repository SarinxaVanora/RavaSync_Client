import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/slot-element.js
var tagName = "slot";
/**
* @implements globalThis.HTMLSlotElement
*/
var HTMLSlotElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
	/* c8 ignore start */
	get name() {
		return this.getAttribute("name");
	}
	set name(value) {
		this.setAttribute("name", value);
	}
	assign() {}
	assignedNodes(options) {
		const isNamedSlot = !!this.name;
		const hostChildNodes = this.getRootNode().host?.childNodes ?? [];
		let slottables;
		if (isNamedSlot) slottables = [...hostChildNodes].filter((node) => node.slot === this.name);
		else slottables = [...hostChildNodes].filter((node) => !node.slot);
		if (options?.flatten) {
			const result = [];
			for (let slottable of slottables) if (slottable.localName === "slot") result.push(...slottable.assignedNodes({ flatten: true }));
			else result.push(slottable);
			slottables = result;
		}
		return slottables.length ? slottables : [...this.childNodes];
	}
	assignedElements(options) {
		const slottables = this.assignedNodes(options).filter((n) => n.nodeType === 1);
		return slottables.length ? slottables : [...this.children];
	}
};
registerHTMLClass(tagName, HTMLSlotElement);
//#endregion
export { HTMLSlotElement };
