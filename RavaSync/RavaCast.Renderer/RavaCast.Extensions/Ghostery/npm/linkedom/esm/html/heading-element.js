import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/heading-element.js
var tagName = "h1";
/**
* @implements globalThis.HTMLHeadingElement
*/
var HTMLHeadingElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
};
registerHTMLClass([
	tagName,
	"h2",
	"h3",
	"h4",
	"h5",
	"h6"
], HTMLHeadingElement);
//#endregion
export { HTMLHeadingElement };
