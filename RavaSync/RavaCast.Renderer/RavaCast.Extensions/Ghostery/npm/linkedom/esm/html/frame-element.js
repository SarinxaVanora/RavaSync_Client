import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/frame-element.js
/**
* @implements globalThis.HTMLFrameElement
*/
var HTMLFrameElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "frame") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLFrameElement };
