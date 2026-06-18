import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/data-list-element.js
/**
* @implements globalThis.HTMLDataListElement
*/
var HTMLDataListElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "datalist") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLDataListElement };
