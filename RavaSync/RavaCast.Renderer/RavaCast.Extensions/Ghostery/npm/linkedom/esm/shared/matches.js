import { ignoreCase } from "./utils.js";
import { compile, is } from "../../../css-select/lib/esm/index.js";
//#region node_modules/linkedom/esm/shared/matches.js
var { isArray } = Array;
/* c8 ignore start */
var isTag = ({ nodeType }) => nodeType === 1;
var existsOne = (test, elements) => elements.some((element) => isTag(element) && (test(element) || existsOne(test, getChildren(element))));
var getAttributeValue = (element, name) => name === "class" ? element.classList.value : element.getAttribute(name);
var getChildren = ({ childNodes }) => childNodes;
var getName = (element) => {
	const { localName } = element;
	return ignoreCase(element) ? localName.toLowerCase() : localName;
};
var getParent = ({ parentNode }) => parentNode;
var getSiblings = (element) => {
	const { parentNode } = element;
	return parentNode ? getChildren(parentNode) : element;
};
var getText = (node) => {
	if (isArray(node)) return node.map(getText).join("");
	if (isTag(node)) return getText(getChildren(node));
	if (node.nodeType === 3) return node.data;
	return "";
};
var hasAttrib = (element, name) => element.hasAttribute(name);
var removeSubsets = (nodes) => {
	let { length } = nodes;
	while (length--) {
		const node = nodes[length];
		if (length && -1 < nodes.lastIndexOf(node, length - 1)) {
			nodes.splice(length, 1);
			continue;
		}
		for (let { parentNode } = node; parentNode; parentNode = parentNode.parentNode) if (nodes.includes(parentNode)) {
			nodes.splice(length, 1);
			break;
		}
	}
	return nodes;
};
var findAll = (test, nodes) => {
	const matches = [];
	for (const node of nodes) if (isTag(node)) {
		if (test(node)) matches.push(node);
		matches.push(...findAll(test, getChildren(node)));
	}
	return matches;
};
var findOne = (test, nodes) => {
	for (let node of nodes) if (test(node) || (node = findOne(test, getChildren(node)))) return node;
	return null;
};
/* c8 ignore stop */
var adapter = {
	isTag,
	existsOne,
	getAttributeValue,
	getChildren,
	getName,
	getParent,
	getSiblings,
	getText,
	hasAttrib,
	removeSubsets,
	findAll,
	findOne
};
var prepareMatch = (element, selectors) => compile(selectors, {
	context: selectors.includes(":scope") ? element : void 0,
	xmlMode: !ignoreCase(element),
	adapter
});
var matches = (element, selectors) => is(element, selectors, {
	strict: true,
	context: selectors.includes(":scope") ? element : void 0,
	xmlMode: !ignoreCase(element),
	adapter
});
//#endregion
export { matches, prepareMatch };
