import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/unknown-element.js
/**
* @implements globalThis.HTMLUnknownElement
*/
var HTMLUnknownElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "unknown") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLUnknownElement };
