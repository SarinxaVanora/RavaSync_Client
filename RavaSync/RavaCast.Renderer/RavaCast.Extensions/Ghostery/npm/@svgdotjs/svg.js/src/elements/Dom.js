import { map } from "../utils/utils.js";
import { html, svg } from "../modules/core/namespaces.js";
import { globals } from "../utils/window.js";
import { adopt, assignNewId, create, eid, extend, makeInstance, register } from "../utils/adopter.js";
import List from "../types/List.js";
import { find, findOne } from "../modules/core/selector.js";
import EventTarget from "../types/EventTarget.js";
import attr from "../modules/core/attr.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Dom.js
var Dom = class Dom extends EventTarget {
	constructor(node, attrs) {
		super();
		this.node = node;
		this.type = node.nodeName;
		if (attrs && node !== attrs) this.attr(attrs);
	}
	add(element, i) {
		element = makeInstance(element);
		if (element.removeNamespace && this.node instanceof globals.window.SVGElement) element.removeNamespace();
		if (i == null) this.node.appendChild(element.node);
		else if (element.node !== this.node.childNodes[i]) this.node.insertBefore(element.node, this.node.childNodes[i]);
		return this;
	}
	addTo(parent, i) {
		return makeInstance(parent).put(this, i);
	}
	children() {
		return new List(map(this.node.children, function(node) {
			return adopt(node);
		}));
	}
	clear() {
		while (this.node.hasChildNodes()) this.node.removeChild(this.node.lastChild);
		return this;
	}
	clone(deep = true, assignNewIds = true) {
		this.writeDataToDom();
		let nodeClone = this.node.cloneNode(deep);
		if (assignNewIds) nodeClone = assignNewId(nodeClone);
		return new this.constructor(nodeClone);
	}
	each(block, deep) {
		const children = this.children();
		let i, il;
		for (i = 0, il = children.length; i < il; i++) {
			block.apply(children[i], [i, children]);
			if (deep) children[i].each(block, deep);
		}
		return this;
	}
	element(nodeName, attrs) {
		return this.put(new Dom(create(nodeName), attrs));
	}
	first() {
		return adopt(this.node.firstChild);
	}
	get(i) {
		return adopt(this.node.childNodes[i]);
	}
	getEventHolder() {
		return this.node;
	}
	getEventTarget() {
		return this.node;
	}
	has(element) {
		return this.index(element) >= 0;
	}
	html(htmlOrFn, outerHTML) {
		return this.xml(htmlOrFn, outerHTML, html);
	}
	id(id) {
		if (typeof id === "undefined" && !this.node.id) this.node.id = eid(this.type);
		return this.attr("id", id);
	}
	index(element) {
		return [].slice.call(this.node.childNodes).indexOf(element.node);
	}
	last() {
		return adopt(this.node.lastChild);
	}
	matches(selector) {
		const el = this.node;
		const matcher = el.matches || el.matchesSelector || el.msMatchesSelector || el.mozMatchesSelector || el.webkitMatchesSelector || el.oMatchesSelector || null;
		return matcher && matcher.call(el, selector);
	}
	parent(type) {
		let parent = this;
		if (!parent.node.parentNode) return null;
		parent = adopt(parent.node.parentNode);
		if (!type) return parent;
		do
			if (typeof type === "string" ? parent.matches(type) : parent instanceof type) return parent;
		while (parent = adopt(parent.node.parentNode));
		return parent;
	}
	put(element, i) {
		element = makeInstance(element);
		this.add(element, i);
		return element;
	}
	putIn(parent, i) {
		return makeInstance(parent).add(this, i);
	}
	remove() {
		if (this.parent()) this.parent().removeElement(this);
		return this;
	}
	removeElement(element) {
		this.node.removeChild(element.node);
		return this;
	}
	replace(element) {
		element = makeInstance(element);
		if (this.node.parentNode) this.node.parentNode.replaceChild(element.node, this.node);
		return element;
	}
	round(precision = 2, map = null) {
		const factor = 10 ** precision;
		const attrs = this.attr(map);
		for (const i in attrs) if (typeof attrs[i] === "number") attrs[i] = Math.round(attrs[i] * factor) / factor;
		this.attr(attrs);
		return this;
	}
	svg(svgOrFn, outerSVG) {
		return this.xml(svgOrFn, outerSVG, svg);
	}
	toString() {
		return this.id();
	}
	words(text) {
		this.node.textContent = text;
		return this;
	}
	wrap(node) {
		const parent = this.parent();
		if (!parent) return this.addTo(node);
		const position = parent.index(this);
		return parent.put(node, position).put(this);
	}
	writeDataToDom() {
		this.each(function() {
			this.writeDataToDom();
		});
		return this;
	}
	xml(xmlOrFn, outerXML, ns) {
		if (typeof xmlOrFn === "boolean") {
			ns = outerXML;
			outerXML = xmlOrFn;
			xmlOrFn = null;
		}
		if (xmlOrFn == null || typeof xmlOrFn === "function") {
			outerXML = outerXML == null ? true : outerXML;
			this.writeDataToDom();
			let current = this;
			if (xmlOrFn != null) {
				current = adopt(current.node.cloneNode(true));
				if (outerXML) {
					const result = xmlOrFn(current);
					current = result || current;
					if (result === false) return "";
				}
				current.each(function() {
					const result = xmlOrFn(this);
					const _this = result || this;
					if (result === false) this.remove();
					else if (result && this !== _this) this.replace(_this);
				}, true);
			}
			return outerXML ? current.node.outerHTML : current.node.innerHTML;
		}
		outerXML = outerXML == null ? false : outerXML;
		const well = create("wrapper", ns);
		const fragment = globals.document.createDocumentFragment();
		well.innerHTML = xmlOrFn;
		for (let len = well.children.length; len--;) fragment.appendChild(well.firstElementChild);
		const parent = this.parent();
		return outerXML ? this.replace(fragment) && parent : this.add(fragment);
	}
};
extend(Dom, {
	attr,
	find,
	findOne
});
register(Dom, "Dom");
//#endregion
export { Dom as default };
