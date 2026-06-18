import { END, NEXT, PREV, PRIVATE, START, VALUE } from "../shared/symbols.js";
import { getEnd, knownAdjacent, knownBoundaries, knownSegment, knownSiblings, localCase } from "../shared/utils.js";
import { connectedCallback } from "../interface/custom-element-registry.js";
import { moCallback } from "../interface/mutation-observer.js";
import { NodeList } from "../interface/node-list.js";
import { Node } from "../interface/node.js";
import { nextSibling, previousSibling } from "../shared/node.js";
import { nextElementSibling } from "./non-document-type-child-node.js";
import { prepareMatch } from "../shared/matches.js";
import { Text } from "../interface/text.js";
//#region node_modules/linkedom/esm/mixin/parent-node.js
var isNode = (node) => node instanceof Node;
var insert = (parentNode, child, nodes) => {
	const { ownerDocument } = parentNode;
	for (const node of nodes) parentNode.insertBefore(isNode(node) ? node : new Text(ownerDocument, node), child);
};
/** @typedef { import('../interface/element.js').Element & {
[typeof NEXT]: NodeStruct,
[typeof PREV]: NodeStruct,
[typeof START]: NodeStruct,
nodeType: typeof ATTRIBUTE_NODE | typeof DOCUMENT_FRAGMENT_NODE | typeof ELEMENT_NODE | typeof TEXT_NODE | typeof NODE_END | typeof COMMENT_NODE | typeof CDATA_SECTION_NODE,
ownerDocument: Document,
parentNode: ParentNode,
}} NodeStruct */
var ParentNode = class extends Node {
	constructor(ownerDocument, localName, nodeType) {
		super(ownerDocument, localName, nodeType);
		this[PRIVATE] = null;
		/** @type {NodeStruct} */
		this[NEXT] = this[END] = {
			[NEXT]: null,
			[PREV]: this,
			[START]: this,
			nodeType: -1,
			ownerDocument: this.ownerDocument,
			parentNode: null
		};
	}
	get childNodes() {
		const childNodes = new NodeList();
		let { firstChild } = this;
		while (firstChild) {
			childNodes.push(firstChild);
			firstChild = nextSibling(firstChild);
		}
		return childNodes;
	}
	get children() {
		const children = new NodeList();
		let { firstElementChild } = this;
		while (firstElementChild) {
			children.push(firstElementChild);
			firstElementChild = nextElementSibling(firstElementChild);
		}
		return children;
	}
	/**
	* @returns {NodeStruct | null}
	*/
	get firstChild() {
		let { [NEXT]: next, [END]: end } = this;
		while (next.nodeType === 2) next = next[NEXT];
		return next === end ? null : next;
	}
	/**
	* @returns {NodeStruct | null}
	*/
	get firstElementChild() {
		let { firstChild } = this;
		while (firstChild) {
			if (firstChild.nodeType === 1) return firstChild;
			firstChild = nextSibling(firstChild);
		}
		return null;
	}
	get lastChild() {
		const prev = this[END][PREV];
		switch (prev.nodeType) {
			case -1: return prev[START];
			case 2: return null;
		}
		return prev === this ? null : prev;
	}
	get lastElementChild() {
		let { lastChild } = this;
		while (lastChild) {
			if (lastChild.nodeType === 1) return lastChild;
			lastChild = previousSibling(lastChild);
		}
		return null;
	}
	get childElementCount() {
		return this.children.length;
	}
	prepend(...nodes) {
		insert(this, this.firstChild, nodes);
	}
	append(...nodes) {
		insert(this, this[END], nodes);
	}
	replaceChildren(...nodes) {
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end && next.nodeType === 2) next = next[NEXT];
		while (next !== end) {
			const after = getEnd(next)[NEXT];
			next.remove();
			next = after;
		}
		if (nodes.length) insert(this, end, nodes);
	}
	getElementsByClassName(className) {
		const elements = new NodeList();
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end) {
			if (next.nodeType === 1 && next.hasAttribute("class") && next.classList.has(className)) elements.push(next);
			next = next[NEXT];
		}
		return elements;
	}
	getElementsByTagName(tagName) {
		const elements = new NodeList();
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end) {
			if (next.nodeType === 1 && (next.localName === tagName || localCase(next) === tagName)) elements.push(next);
			next = next[NEXT];
		}
		return elements;
	}
	querySelector(selectors) {
		const matches = prepareMatch(this, selectors);
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end) {
			if (next.nodeType === 1 && matches(next)) return next;
			next = next.nodeType === 1 && next.localName === "template" ? next[END] : next[NEXT];
		}
		return null;
	}
	querySelectorAll(selectors) {
		const matches = prepareMatch(this, selectors);
		const elements = new NodeList();
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end) {
			if (next.nodeType === 1 && matches(next)) elements.push(next);
			next = next.nodeType === 1 && next.localName === "template" ? next[END] : next[NEXT];
		}
		return elements;
	}
	appendChild(node) {
		return this.insertBefore(node, this[END]);
	}
	contains(node) {
		let parentNode = node;
		while (parentNode && parentNode !== this) parentNode = parentNode.parentNode;
		return parentNode === this;
	}
	insertBefore(node, before = null) {
		if (node === before) return node;
		if (node === this) throw new Error("unable to append a node to itself");
		const next = before || this[END];
		switch (node.nodeType) {
			case 1:
				node.remove();
				node.parentNode = this;
				knownBoundaries(next[PREV], node, next);
				moCallback(node, null);
				connectedCallback(node);
				break;
			case 11: {
				let { [PRIVATE]: parentNode, firstChild, lastChild } = node;
				if (firstChild) {
					knownSegment(next[PREV], firstChild, lastChild, next);
					knownAdjacent(node, node[END]);
					if (parentNode) parentNode.replaceChildren();
					do {
						firstChild.parentNode = this;
						moCallback(firstChild, null);
						if (firstChild.nodeType === 1) connectedCallback(firstChild);
					} while (firstChild !== lastChild && (firstChild = nextSibling(firstChild)));
				}
				break;
			}
			case 3:
			case 8:
			case 4: node.remove();
			default:
				node.parentNode = this;
				knownSiblings(next[PREV], node, next);
				moCallback(node, null);
				break;
		}
		return node;
	}
	normalize() {
		let { [NEXT]: next, [END]: end } = this;
		while (next !== end) {
			const { [NEXT]: $next, [PREV]: $prev, nodeType } = next;
			if (nodeType === 3) {
				if (!next[VALUE]) next.remove();
				else if ($prev && $prev.nodeType === 3) {
					$prev.textContent += next.textContent;
					next.remove();
				}
			}
			next = $next;
		}
	}
	removeChild(node) {
		if (node.parentNode !== this) throw new Error("node is not a child");
		node.remove();
		return node;
	}
	replaceChild(node, replaced) {
		const next = getEnd(replaced)[NEXT];
		replaced.remove();
		this.insertBefore(node, next);
		return replaced;
	}
};
//#endregion
export { ParentNode };
