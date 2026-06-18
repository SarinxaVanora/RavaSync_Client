import { END, MIME, NEXT, PREV } from "./symbols.js";
//#region node_modules/linkedom/esm/shared/utils.js
var $String = String;
var getEnd = (node) => node.nodeType === 1 ? node[END] : node;
var ignoreCase = ({ ownerDocument }) => ownerDocument[MIME].ignoreCase;
var knownAdjacent = (prev, next) => {
	prev[NEXT] = next;
	next[PREV] = prev;
};
var knownBoundaries = (prev, current, next) => {
	knownAdjacent(prev, current);
	knownAdjacent(getEnd(current), next);
};
var knownSegment = (prev, start, end, next) => {
	knownAdjacent(prev, start);
	knownAdjacent(getEnd(end), next);
};
var knownSiblings = (prev, current, next) => {
	knownAdjacent(prev, current);
	knownAdjacent(current, next);
};
var localCase = ({ localName, ownerDocument }) => {
	return ownerDocument[MIME].ignoreCase ? localName.toUpperCase() : localName;
};
var setAdjacent = (prev, next) => {
	if (prev) prev[NEXT] = next;
	if (next) next[PREV] = prev;
};
/**
* @param {import("../interface/document.js").Document} ownerDocument
* @param {string} html
* @return {import("../interface/document-fragment.js").DocumentFragment}
*/
var htmlToFragment = (ownerDocument, html) => {
	const fragment = ownerDocument.createDocumentFragment();
	const elem = ownerDocument.createElement("");
	elem.innerHTML = html;
	const { firstChild, lastChild } = elem;
	if (firstChild) {
		knownSegment(fragment, firstChild, lastChild, fragment[END]);
		let child = firstChild;
		do
			child.parentNode = fragment;
		while (child !== lastChild && (child = getEnd(child)[NEXT]));
	}
	return fragment;
};
//#endregion
export { $String, getEnd, htmlToFragment, ignoreCase, knownAdjacent, knownBoundaries, knownSegment, knownSiblings, localCase, setAdjacent };
