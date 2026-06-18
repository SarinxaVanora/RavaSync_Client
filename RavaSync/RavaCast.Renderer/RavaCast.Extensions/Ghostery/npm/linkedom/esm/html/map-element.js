import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/map-element.js
/**
* @implements globalThis.HTMLMapElement
*/
var HTMLMapElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "map") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLMapElement };
