import { NEXT, PREV, START } from "./symbols.js";
import { getEnd } from "./utils.js";
//#region node_modules/linkedom/esm/shared/node.js
var isConnected = ({ ownerDocument, parentNode }) => {
	while (parentNode) {
		if (parentNode === ownerDocument) return true;
		parentNode = parentNode.parentNode || parentNode.host;
	}
	return false;
};
var parentElement = ({ parentNode }) => {
	if (parentNode) switch (parentNode.nodeType) {
		case 9:
		case 11: return null;
	}
	return parentNode;
};
var previousSibling = ({ [PREV]: prev }) => {
	switch (prev ? prev.nodeType : 0) {
		case -1: return prev[START];
		case 3:
		case 8:
		case 4: return prev;
	}
	return null;
};
var nextSibling = (node) => {
	const next = getEnd(node)[NEXT];
	return next && (next.nodeType === -1 ? null : next);
};
//#endregion
export { isConnected, nextSibling, parentElement, previousSibling };
