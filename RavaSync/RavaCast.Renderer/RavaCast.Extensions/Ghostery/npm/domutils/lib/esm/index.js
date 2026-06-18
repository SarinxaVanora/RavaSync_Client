import { __exportAll } from "../../../../virtual/_rolldown/runtime.js";
import { hasChildren, isCDATA, isComment, isDocument, isTag, isText } from "../../../domhandler/lib/esm/node.js";
import { getInnerHTML, getOuterHTML, getText, innerText, textContent } from "./stringify.js";
import { getAttributeValue, getChildren, getName, getParent, getSiblings, hasAttrib, nextElementSibling, prevElementSibling } from "./traversal.js";
import { append, appendChild, prepend, prependChild, removeElement, replaceElement } from "./manipulation.js";
import { existsOne, filter, find, findAll, findOne, findOneChild } from "./querying.js";
import { getElementById, getElements, getElementsByClassName, getElementsByTagName, getElementsByTagType, testElement } from "./legacy.js";
import { DocumentPosition, compareDocumentPosition, removeSubsets, uniqueSort } from "./helpers.js";
import { getFeed } from "./feeds.js";
//#region node_modules/domutils/lib/esm/index.js
var esm_exports = /* @__PURE__ */ __exportAll({
	DocumentPosition: () => DocumentPosition,
	append: () => append,
	appendChild: () => appendChild,
	compareDocumentPosition: () => compareDocumentPosition,
	existsOne: () => existsOne,
	filter: () => filter,
	find: () => find,
	findAll: () => findAll,
	findOne: () => findOne,
	findOneChild: () => findOneChild,
	getAttributeValue: () => getAttributeValue,
	getChildren: () => getChildren,
	getElementById: () => getElementById,
	getElements: () => getElements,
	getElementsByClassName: () => getElementsByClassName,
	getElementsByTagName: () => getElementsByTagName,
	getElementsByTagType: () => getElementsByTagType,
	getFeed: () => getFeed,
	getInnerHTML: () => getInnerHTML,
	getName: () => getName,
	getOuterHTML: () => getOuterHTML,
	getParent: () => getParent,
	getSiblings: () => getSiblings,
	getText: () => getText,
	hasAttrib: () => hasAttrib,
	hasChildren: () => hasChildren,
	innerText: () => innerText,
	isCDATA: () => isCDATA,
	isComment: () => isComment,
	isDocument: () => isDocument,
	isTag: () => isTag,
	isText: () => isText,
	nextElementSibling: () => nextElementSibling,
	prepend: () => prepend,
	prependChild: () => prependChild,
	prevElementSibling: () => prevElementSibling,
	removeElement: () => removeElement,
	removeSubsets: () => removeSubsets,
	replaceElement: () => replaceElement,
	testElement: () => testElement,
	textContent: () => textContent,
	uniqueSort: () => uniqueSort
});
//#endregion
export { esm_exports };
