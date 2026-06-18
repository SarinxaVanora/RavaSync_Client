import { nextSibling, previousSibling } from "../shared/node.js";
//#region node_modules/linkedom/esm/mixin/non-document-type-child-node.js
var nextElementSibling = (node) => {
	let next = nextSibling(node);
	while (next && next.nodeType !== 1) next = nextSibling(next);
	return next;
};
var previousElementSibling = (node) => {
	let prev = previousSibling(node);
	while (prev && prev.nodeType !== 1) prev = previousSibling(prev);
	return prev;
};
//#endregion
export { nextElementSibling, previousElementSibling };
