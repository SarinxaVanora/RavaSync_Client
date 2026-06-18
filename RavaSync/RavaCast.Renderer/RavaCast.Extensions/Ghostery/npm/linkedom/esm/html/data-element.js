import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/data-element.js
/**
* @implements globalThis.HTMLDataElement
*/
var HTMLDataElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "data") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLDataElement };
