import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/object-element.js
/**
* @implements globalThis.HTMLObjectElement
*/
var HTMLObjectElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "object") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLObjectElement };
