import { CONTENT, PRIVATE } from "../shared/symbols.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/template-element.js
var tagName = "template";
/**
* @implements globalThis.HTMLTemplateElement
*/
var HTMLTemplateElement = class extends HTMLElement {
	constructor(ownerDocument) {
		super(ownerDocument, tagName);
		const content = this.ownerDocument.createDocumentFragment();
		(this[CONTENT] = content)[PRIVATE] = this;
	}
	get content() {
		if (this.hasChildNodes() && !this[CONTENT].hasChildNodes()) for (const node of this.childNodes) this[CONTENT].appendChild(node.cloneNode(true));
		return this[CONTENT];
	}
};
registerHTMLClass(tagName, HTMLTemplateElement);
//#endregion
export { HTMLTemplateElement };
