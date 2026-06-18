import { CUSTOM_ELEMENTS } from "../shared/symbols.js";
import { ignoreCase } from "../shared/utils.js";
import { parseFromString } from "../shared/parse-from-string.js";
//#region node_modules/linkedom/esm/mixin/inner-html.js
/**
* @param {Node} node
* @returns {String}
*/
var getInnerHtml = (node) => node.childNodes.join("");
/**
* @param {Node} node
* @param {String} html
*/
var setInnerHtml = (node, html) => {
	const { ownerDocument } = node;
	const { constructor } = ownerDocument;
	const document = new constructor();
	document[CUSTOM_ELEMENTS] = ownerDocument[CUSTOM_ELEMENTS];
	const { childNodes } = parseFromString(document, ignoreCase(node), html);
	node.replaceChildren(...childNodes.map(setOwnerDocument, ownerDocument));
};
function setOwnerDocument(node) {
	node.ownerDocument = this;
	switch (node.nodeType) {
		case 1:
		case 11:
			node.childNodes.forEach(setOwnerDocument, this);
			break;
	}
	return node;
}
//#endregion
export { getInnerHtml, setInnerHtml };
