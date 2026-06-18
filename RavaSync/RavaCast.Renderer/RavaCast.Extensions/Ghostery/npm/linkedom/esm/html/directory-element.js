import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/directory-element.js
/**
* @implements globalThis.HTMLDirectoryElement
*/
var HTMLDirectoryElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "dir") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLDirectoryElement };
