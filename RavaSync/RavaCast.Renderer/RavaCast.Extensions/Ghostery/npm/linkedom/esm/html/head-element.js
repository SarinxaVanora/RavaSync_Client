import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/head-element.js
/**
* @implements globalThis.HTMLHeadElement
*/
var HTMLHeadElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "head") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLHeadElement };
