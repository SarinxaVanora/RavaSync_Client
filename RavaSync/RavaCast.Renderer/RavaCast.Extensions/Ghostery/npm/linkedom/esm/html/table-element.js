import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/table-element.js
/**
* @implements globalThis.HTMLTableElement
*/
var HTMLTableElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "table") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLTableElement };
