import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/form-element.js
/**
* @implements globalThis.HTMLFormElement
*/
var HTMLFormElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "form") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLFormElement };
