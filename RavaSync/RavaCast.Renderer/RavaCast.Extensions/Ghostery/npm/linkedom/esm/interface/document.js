import "../shared/constants.js";
import { assign, create, defineProperties, setPrototypeOf } from "../shared/object.js";
import { CUSTOM_ELEMENTS, DOCTYPE, DOM_PARSER, END, EVENT_TARGET, GLOBALS, IMAGE, MIME, MUTATION_OBSERVER, NEXT, UPGRADE } from "../shared/symbols.js";
import { knownSiblings } from "../shared/utils.js";
import { CustomElementRegistry } from "./custom-element-registry.js";
import { MutationObserverClass } from "./mutation-observer.js";
import { DOMEventTarget } from "./event-target.js";
import { NodeList } from "./node-list.js";
import { Attr } from "./attr.js";
import { Text } from "./text.js";
import { GlobalEvent } from "./event.js";
import { NamedNodeMap } from "./named-node-map.js";
import { NonElementParentNode } from "../mixin/non-element-parent-node.js";
import { Element } from "./element.js";
import { CDATASection } from "./cdata-section.js";
import { Comment } from "./comment.js";
import { DocumentFragment } from "./document-fragment.js";
import { DocumentType } from "./document-type.js";
import { SVGElement } from "../svg/element.js";
import { Facades, illegalConstructor } from "../shared/facades.js";
import { HTMLClasses } from "../shared/html-classes.js";
import { Mime } from "../shared/mime.js";
import { CustomEvent } from "./custom-event.js";
import { InputEvent } from "./input-event.js";
import { ImageClass } from "./image.js";
import { Range } from "./range.js";
import { TreeWalker } from "./tree-walker.js";
//#region node_modules/linkedom/esm/interface/document.js
var query = (method, ownerDocument, selectors) => {
	let { [NEXT]: next, [END]: end } = ownerDocument;
	return method.call({
		ownerDocument,
		[NEXT]: next,
		[END]: end
	}, selectors);
};
var globalExports = assign({}, Facades, HTMLClasses, {
	CustomEvent,
	Event: GlobalEvent,
	EventTarget: DOMEventTarget,
	InputEvent,
	NamedNodeMap,
	NodeList
});
var window = /* @__PURE__ */ new WeakMap();
/**
* @implements globalThis.Document
*/
var Document = class extends NonElementParentNode {
	constructor(type) {
		super(null, "#document", 9);
		this[CUSTOM_ELEMENTS] = {
			active: false,
			registry: null
		};
		this[MUTATION_OBSERVER] = {
			active: false,
			class: null
		};
		this[MIME] = Mime[type];
		/** @type {DocumentType} */
		this[DOCTYPE] = null;
		this[DOM_PARSER] = null;
		this[GLOBALS] = null;
		this[IMAGE] = null;
		this[UPGRADE] = null;
	}
	/**
	* @type {globalThis.Document['defaultView']}
	*/
	get defaultView() {
		if (!window.has(this)) window.set(this, new Proxy(globalThis, {
			set: (target, name, value) => {
				switch (name) {
					case "addEventListener":
					case "removeEventListener":
					case "dispatchEvent":
						this[EVENT_TARGET][name] = value;
						break;
					default:
						target[name] = value;
						break;
				}
				return true;
			},
			get: (globalThis, name) => {
				switch (name) {
					case "addEventListener":
					case "removeEventListener":
					case "dispatchEvent":
						if (!this[EVENT_TARGET]) {
							const et = this[EVENT_TARGET] = new DOMEventTarget();
							et.dispatchEvent = et.dispatchEvent.bind(et);
							et.addEventListener = et.addEventListener.bind(et);
							et.removeEventListener = et.removeEventListener.bind(et);
						}
						return this[EVENT_TARGET][name];
					case "document": return this;
					/* c8 ignore start */
					case "navigator": return { userAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36" };
					/* c8 ignore stop */
					case "window": return window.get(this);
					case "customElements":
						if (!this[CUSTOM_ELEMENTS].registry) this[CUSTOM_ELEMENTS] = new CustomElementRegistry(this);
						return this[CUSTOM_ELEMENTS];
					case "performance": return globalThis.performance;
					case "DOMParser": return this[DOM_PARSER];
					case "Image":
						if (!this[IMAGE]) this[IMAGE] = ImageClass(this);
						return this[IMAGE];
					case "MutationObserver":
						if (!this[MUTATION_OBSERVER].class) this[MUTATION_OBSERVER] = new MutationObserverClass(this);
						return this[MUTATION_OBSERVER].class;
				}
				return this[GLOBALS] && this[GLOBALS][name] || globalExports[name] || globalThis[name];
			}
		}));
		return window.get(this);
	}
	get doctype() {
		const docType = this[DOCTYPE];
		if (docType) return docType;
		const { firstChild } = this;
		if (firstChild && firstChild.nodeType === 10) return this[DOCTYPE] = firstChild;
		return null;
	}
	set doctype(value) {
		if (/^([a-z:]+)(\s+system|\s+public(\s+"([^"]+)")?)?(\s+"([^"]+)")?/i.test(value)) {
			const { $1: name, $4: publicId, $6: systemId } = RegExp;
			this[DOCTYPE] = new DocumentType(this, name, publicId, systemId);
			knownSiblings(this, this[DOCTYPE], this[NEXT]);
		}
	}
	get documentElement() {
		return this.firstElementChild;
	}
	get isConnected() {
		return true;
	}
	/**
	* @protected
	*/
	_getParent() {
		return this[EVENT_TARGET];
	}
	createAttribute(name) {
		return new Attr(this, name);
	}
	createCDATASection(data) {
		return new CDATASection(this, data);
	}
	createComment(textContent) {
		return new Comment(this, textContent);
	}
	createDocumentFragment() {
		return new DocumentFragment(this);
	}
	createDocumentType(name, publicId, systemId) {
		return new DocumentType(this, name, publicId, systemId);
	}
	createElement(localName) {
		return new Element(this, localName);
	}
	createRange() {
		const range = new Range();
		range.commonAncestorContainer = this;
		return range;
	}
	createTextNode(textContent) {
		return new Text(this, textContent);
	}
	createTreeWalker(root, whatToShow = -1) {
		return new TreeWalker(root, whatToShow);
	}
	createNodeIterator(root, whatToShow = -1) {
		return this.createTreeWalker(root, whatToShow);
	}
	createEvent(name) {
		const event = create(name === "Event" ? new GlobalEvent("") : new CustomEvent(""));
		event.initEvent = event.initCustomEvent = (type, canBubble = false, cancelable = false, detail) => {
			event.bubbles = !!canBubble;
			defineProperties(event, {
				type: { value: type },
				canBubble: { value: canBubble },
				cancelable: { value: cancelable },
				detail: { value: detail }
			});
		};
		return event;
	}
	cloneNode(deep = false) {
		const { constructor, [CUSTOM_ELEMENTS]: customElements, [DOCTYPE]: doctype } = this;
		const document = new constructor();
		document[CUSTOM_ELEMENTS] = customElements;
		if (deep) {
			const end = document[END];
			const { childNodes } = this;
			for (let { length } = childNodes, i = 0; i < length; i++) document.insertBefore(childNodes[i].cloneNode(true), end);
			if (doctype) document[DOCTYPE] = childNodes[0];
		}
		return document;
	}
	importNode(externalNode) {
		const deep = 1 < arguments.length && !!arguments[1];
		const node = externalNode.cloneNode(deep);
		const { [CUSTOM_ELEMENTS]: customElements } = this;
		const { active } = customElements;
		const upgrade = (element) => {
			const { ownerDocument, nodeType } = element;
			element.ownerDocument = this;
			if (active && ownerDocument !== this && nodeType === 1) customElements.upgrade(element);
		};
		upgrade(node);
		if (deep) switch (node.nodeType) {
			case 1:
			case 11: {
				let { [NEXT]: next, [END]: end } = node;
				while (next !== end) {
					if (next.nodeType === 1) upgrade(next);
					next = next[NEXT];
				}
				break;
			}
		}
		return node;
	}
	toString() {
		return this.childNodes.join("");
	}
	querySelector(selectors) {
		return query(super.querySelector, this, selectors);
	}
	querySelectorAll(selectors) {
		return query(super.querySelectorAll, this, selectors);
	}
	/* c8 ignore start */
	getElementsByTagNameNS(_, name) {
		return this.getElementsByTagName(name);
	}
	createAttributeNS(_, name) {
		return this.createAttribute(name);
	}
	createElementNS(nsp, localName, options) {
		return nsp === "http://www.w3.org/2000/svg" ? new SVGElement(this, localName, null) : this.createElement(localName, options);
	}
};
setPrototypeOf(globalExports.Document = function Document() {
	illegalConstructor();
}, Document).prototype = Document.prototype;
//#endregion
export { Document };
