import { NEXT, PREV } from "../shared/symbols.js";
import { DOMEventTarget } from "./event-target.js";
import { NodeList } from "./node-list.js";
//#region node_modules/linkedom/esm/interface/node.js
var getParentNodeCount = ({ parentNode }) => {
	let count = 0;
	while (parentNode) {
		count++;
		parentNode = parentNode.parentNode;
	}
	return count;
};
/**
* @implements globalThis.Node
*/
var Node = class extends DOMEventTarget {
	static get ELEMENT_NODE() {
		return 1;
	}
	static get ATTRIBUTE_NODE() {
		return 2;
	}
	static get TEXT_NODE() {
		return 3;
	}
	static get CDATA_SECTION_NODE() {
		return 4;
	}
	static get COMMENT_NODE() {
		return 8;
	}
	static get DOCUMENT_NODE() {
		return 9;
	}
	static get DOCUMENT_FRAGMENT_NODE() {
		return 11;
	}
	static get DOCUMENT_TYPE_NODE() {
		return 10;
	}
	constructor(ownerDocument, localName, nodeType) {
		super();
		this.ownerDocument = ownerDocument;
		this.localName = localName;
		this.nodeType = nodeType;
		this.parentNode = null;
		this[NEXT] = null;
		this[PREV] = null;
	}
	get ELEMENT_NODE() {
		return 1;
	}
	get ATTRIBUTE_NODE() {
		return 2;
	}
	get TEXT_NODE() {
		return 3;
	}
	get CDATA_SECTION_NODE() {
		return 4;
	}
	get COMMENT_NODE() {
		return 8;
	}
	get DOCUMENT_NODE() {
		return 9;
	}
	get DOCUMENT_FRAGMENT_NODE() {
		return 11;
	}
	get DOCUMENT_TYPE_NODE() {
		return 10;
	}
	get baseURI() {
		const ownerDocument = this.nodeType === 9 ? this : this.ownerDocument;
		if (ownerDocument) {
			const base = ownerDocument.querySelector("base");
			if (base) return base.getAttribute("href");
			const { location } = ownerDocument.defaultView;
			if (location) return location.href;
		}
		return null;
	}
	/* c8 ignore start */
	get isConnected() {
		return false;
	}
	get nodeName() {
		return this.localName;
	}
	get parentElement() {
		return null;
	}
	get previousSibling() {
		return null;
	}
	get previousElementSibling() {
		return null;
	}
	get nextSibling() {
		return null;
	}
	get nextElementSibling() {
		return null;
	}
	get childNodes() {
		return new NodeList();
	}
	get firstChild() {
		return null;
	}
	get lastChild() {
		return null;
	}
	get nodeValue() {
		return null;
	}
	set nodeValue(value) {}
	get textContent() {
		return null;
	}
	set textContent(value) {}
	normalize() {}
	cloneNode() {
		return null;
	}
	contains() {
		return false;
	}
	/**
	* Inserts a node before a reference node as a child of this parent node.
	* @param {Node} newNode The node to be inserted.
	* @param {Node} referenceNode The node before which newNode is inserted. If this is null, then newNode is inserted at the end of node's child nodes.
	* @returns The added child
	*/
	insertBefore(newNode, referenceNode) {
		return newNode;
	}
	/**
	* Adds a node to the end of the list of children of this node.
	* @param {Node} child The node to append to the given parent node.
	* @returns The appended child.
	*/
	appendChild(child) {
		return child;
	}
	/**
	* Replaces a child node within this node
	* @param {Node} newChild The new node to replace oldChild.
	* @param {Node} oldChild The child to be replaced.
	* @returns The replaced Node. This is the same node as oldChild.
	*/
	replaceChild(newChild, oldChild) {
		return oldChild;
	}
	/**
	* Removes a child node from the DOM.
	* @param {Node} child A Node that is the child node to be removed from the DOM.
	* @returns The removed node.
	*/
	removeChild(child) {
		return child;
	}
	toString() {
		return "";
	}
	/* c8 ignore stop */
	hasChildNodes() {
		return !!this.lastChild;
	}
	isSameNode(node) {
		return this === node;
	}
	compareDocumentPosition(target) {
		let result = 0;
		if (this !== target) {
			let self = getParentNodeCount(this);
			let other = getParentNodeCount(target);
			if (self < other) {
				result += 4;
				if (this.contains(target)) result += 16;
			} else if (other < self) {
				result += 2;
				if (target.contains(this)) result += 8;
			} else if (self && other) {
				const { childNodes } = this.parentNode;
				if (childNodes.indexOf(this) < childNodes.indexOf(target)) result += 4;
				else result += 2;
			}
			if (!self || !other) {
				result += 32;
				result += 1;
			}
		}
		return result;
	}
	isEqualNode(node) {
		if (this === node) return true;
		if (this.nodeType === node.nodeType) {
			switch (this.nodeType) {
				case 9:
				case 11: {
					const aNodes = this.childNodes;
					const bNodes = node.childNodes;
					return aNodes.length === bNodes.length && aNodes.every((node, i) => node.isEqualNode(bNodes[i]));
				}
			}
			return this.toString() === node.toString();
		}
		return false;
	}
	/**
	* @protected
	*/
	_getParent() {
		return this.parentNode;
	}
	/**
	* Calling it on an element inside a standard web page will return an HTMLDocument object representing the entire page (or <iframe>).
	* Calling it on an element inside a shadow DOM will return the associated ShadowRoot.
	* @return {ShadowRoot | HTMLDocument}
	*/
	getRootNode() {
		let root = this;
		while (root.parentNode) root = root.parentNode;
		return root;
	}
};
//#endregion
export { Node };
