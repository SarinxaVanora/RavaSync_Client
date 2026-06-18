import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/html-element.js
/**
* @implements globalThis.HTMLHtmlElement
*/
var HTMLHtmlElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "html") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLHtmlElement };
