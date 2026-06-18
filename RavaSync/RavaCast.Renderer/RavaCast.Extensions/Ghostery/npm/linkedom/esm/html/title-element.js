import { registerHTMLClass } from "../shared/register-html-class.js";
import { TextElement } from "./text-element.js";
//#region node_modules/linkedom/esm/html/title-element.js
var tagName = "title";
/**
* @implements globalThis.HTMLTitleElement
*/
var HTMLTitleElement = class extends TextElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
	}
};
registerHTMLClass(tagName, HTMLTitleElement);
//#endregion
export { HTMLTitleElement };
