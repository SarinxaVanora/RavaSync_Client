import { NEXT, PREV } from "../shared/symbols.js";
import { getEnd, setAdjacent } from "../shared/utils.js";
import { disconnectedCallback } from "../interface/custom-element-registry.js";
import { moCallback } from "../interface/mutation-observer.js";
//#region node_modules/linkedom/esm/mixin/child-node.js
var asFragment = (ownerDocument, nodes) => {
	const fragment = ownerDocument.createDocumentFragment();
	fragment.append(...nodes);
	return fragment;
};
var before = (node, nodes) => {
	const { ownerDocument, parentNode } = node;
	if (parentNode) parentNode.insertBefore(asFragment(ownerDocument, nodes), node);
};
var after = (node, nodes) => {
	const { ownerDocument, parentNode } = node;
	if (parentNode) parentNode.insertBefore(asFragment(ownerDocument, nodes), getEnd(node)[NEXT]);
};
var replaceWith = (node, nodes) => {
	const { ownerDocument, parentNode } = node;
	if (parentNode) {
		if (nodes.includes(node)) replaceWith(node, [node = node.cloneNode()]);
		parentNode.insertBefore(asFragment(ownerDocument, nodes), node);
		node.remove();
	}
};
var remove = (prev, current, next) => {
	const { parentNode, nodeType } = current;
	if (prev || next) {
		setAdjacent(prev, next);
		current[PREV] = null;
		getEnd(current)[NEXT] = null;
	}
	if (parentNode) {
		current.parentNode = null;
		moCallback(current, parentNode);
		if (nodeType === 1) disconnectedCallback(current);
	}
};
//#endregion
export { after, before, remove, replaceWith };
