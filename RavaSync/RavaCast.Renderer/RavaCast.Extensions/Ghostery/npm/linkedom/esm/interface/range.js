import { END, NEXT, PREV, START } from "../shared/symbols.js";
import { getEnd, htmlToFragment, setAdjacent } from "../shared/utils.js";
import { SVGElement } from "../svg/element.js";
//#region node_modules/linkedom/esm/interface/range.js
var deleteContents = ({ [START]: start, [END]: end }, fragment = null) => {
	setAdjacent(start[PREV], end[NEXT]);
	do {
		const after = getEnd(start);
		const next = after === end ? after : after[NEXT];
		if (fragment) fragment.insertBefore(start, fragment[END]);
		else start.remove();
		start = next;
	} while (start !== end);
};
/**
* @implements globalThis.Range
*/
var Range = class Range {
	constructor() {
		this[START] = null;
		this[END] = null;
		this.commonAncestorContainer = null;
	}
	insertNode(newNode) {
		this[END].parentNode.insertBefore(newNode, this[START]);
	}
	selectNode(node) {
		this[START] = node;
		this[END] = getEnd(node);
	}
	selectNodeContents(node) {
		this.selectNode(node);
		this.commonAncestorContainer = node;
	}
	surroundContents(parentNode) {
		parentNode.replaceChildren(this.extractContents());
	}
	setStartBefore(node) {
		this[START] = node;
	}
	setStartAfter(node) {
		this[START] = node.nextSibling;
	}
	setEndBefore(node) {
		this[END] = getEnd(node.previousSibling);
	}
	setEndAfter(node) {
		this[END] = getEnd(node);
	}
	cloneContents() {
		let { [START]: start, [END]: end } = this;
		const fragment = start.ownerDocument.createDocumentFragment();
		while (start !== end) {
			fragment.insertBefore(start.cloneNode(true), fragment[END]);
			start = getEnd(start);
			if (start !== end) start = start[NEXT];
		}
		return fragment;
	}
	deleteContents() {
		deleteContents(this);
	}
	extractContents() {
		const fragment = this[START].ownerDocument.createDocumentFragment();
		deleteContents(this, fragment);
		return fragment;
	}
	createContextualFragment(html) {
		const { commonAncestorContainer: doc } = this;
		const isSVG = "ownerSVGElement" in doc;
		const document = isSVG ? doc.ownerDocument : doc;
		let content = htmlToFragment(document, html);
		if (isSVG) {
			const childNodes = [...content.childNodes];
			content = document.createDocumentFragment();
			Object.setPrototypeOf(content, SVGElement.prototype);
			content.ownerSVGElement = document;
			for (const child of childNodes) {
				Object.setPrototypeOf(child, SVGElement.prototype);
				child.ownerSVGElement = document;
				content.appendChild(child);
			}
		} else this.selectNode(content);
		return content;
	}
	cloneRange() {
		const range = new Range();
		range[START] = this[START];
		range[END] = this[END];
		return range;
	}
};
//#endregion
export { Range };
