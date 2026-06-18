import { NonElementParentNode } from "../mixin/non-element-parent-node.js";
//#region node_modules/linkedom/esm/interface/document-fragment.js
/**
* @implements globalThis.DocumentFragment
*/
var DocumentFragment = class extends NonElementParentNode {
	constructor(ownerDocument) {
		super(ownerDocument, "#document-fragment", 11);
	}
};
//#endregion
export { DocumentFragment };
