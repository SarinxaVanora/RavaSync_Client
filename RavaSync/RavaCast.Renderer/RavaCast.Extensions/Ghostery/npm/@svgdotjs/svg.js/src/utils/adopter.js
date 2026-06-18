import { addMethodNames } from "./methods.js";
import { capitalize } from "./utils.js";
import { svg } from "../modules/core/namespaces.js";
import { globals } from "./window.js";
import Base from "../types/Base.js";
//#region node_modules/@svgdotjs/svg.js/src/utils/adopter.js
var elements = {};
var root = "___SYMBOL___ROOT___";
function create(name, ns = svg) {
	return globals.document.createElementNS(ns, name);
}
function makeInstance(element, isHTML = false) {
	if (element instanceof Base) return element;
	if (typeof element === "object") return adopter(element);
	if (element == null) return new elements[root]();
	if (typeof element === "string" && element.trim().charAt(0) !== "<") return adopter(globals.document.querySelector(element));
	const wrapper = isHTML ? globals.document.createElement("div") : create("svg");
	wrapper.innerHTML = element.trim();
	element = adopter(wrapper.firstElementChild);
	wrapper.removeChild(wrapper.firstElementChild);
	return element;
}
function nodeOrNew(name, node) {
	return node && (node instanceof globals.window.Node || node.ownerDocument && node instanceof node.ownerDocument.defaultView.Node) ? node : create(name);
}
function adopt(node) {
	if (!node) return null;
	if (node.instance instanceof Base) return node.instance;
	if (node.nodeName === "#document-fragment") return new elements.Fragment(node);
	let className = capitalize(node.nodeName || "Dom");
	if (className === "LinearGradient" || className === "RadialGradient") className = "Gradient";
	else if (!elements[className]) className = "Dom";
	return new elements[className](node);
}
var adopter = adopt;
function register(element, name = element.name, asRoot = false) {
	elements[name] = element;
	if (asRoot) elements[root] = element;
	addMethodNames(Object.getOwnPropertyNames(element.prototype));
	return element;
}
function getClass(name) {
	return elements[name];
}
var did = 1e3;
function eid(name) {
	return "Svgjs" + capitalize(name) + did++;
}
function assignNewId(node) {
	for (let i = node.children.length - 1; i >= 0; i--) assignNewId(node.children[i]);
	if (node.id) {
		node.id = eid(node.nodeName);
		return node;
	}
	return node;
}
function extend(modules, methods) {
	let key, i;
	modules = Array.isArray(modules) ? modules : [modules];
	for (i = modules.length - 1; i >= 0; i--) for (key in methods) modules[i].prototype[key] = methods[key];
}
function wrapWithAttrCheck(fn) {
	return function(...args) {
		const o = args[args.length - 1];
		if (o && o.constructor === Object && !(o instanceof Array)) return fn.apply(this, args.slice(0, -1)).attr(o);
		else return fn.apply(this, args);
	};
}
//#endregion
export { adopt, assignNewId, create, eid, extend, getClass, makeInstance, nodeOrNew, register, root, wrapWithAttrCheck };
