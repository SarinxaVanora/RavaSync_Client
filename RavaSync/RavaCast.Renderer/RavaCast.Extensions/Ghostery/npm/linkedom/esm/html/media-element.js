import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/media-element.js
/**
* @implements globalThis.HTMLMediaElement
*/
var HTMLMediaElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "media") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLMediaElement };
