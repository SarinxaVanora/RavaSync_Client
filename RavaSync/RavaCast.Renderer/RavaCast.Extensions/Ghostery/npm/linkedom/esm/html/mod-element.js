import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/mod-element.js
/**
* @implements globalThis.HTMLModElement
*/
var HTMLModElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "mod") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLModElement };
