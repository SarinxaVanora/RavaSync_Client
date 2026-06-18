import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/body-element.js
/**
* @implements globalThis.HTMLBodyElement
*/
var HTMLBodyElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "body") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLBodyElement };
