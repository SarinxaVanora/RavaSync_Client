import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/base-element.js
/**
* @implements globalThis.HTMLBaseElement
*/
var HTMLBaseElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "base") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLBaseElement };
