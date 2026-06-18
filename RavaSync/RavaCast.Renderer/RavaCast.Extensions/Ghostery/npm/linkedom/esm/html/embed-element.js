import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/embed-element.js
/**
* @implements globalThis.HTMLEmbedElement
*/
var HTMLEmbedElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "embed") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLEmbedElement };
