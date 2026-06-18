import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/output-element.js
/**
* @implements globalThis.HTMLOutputElement
*/
var HTMLOutputElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "output") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLOutputElement };
