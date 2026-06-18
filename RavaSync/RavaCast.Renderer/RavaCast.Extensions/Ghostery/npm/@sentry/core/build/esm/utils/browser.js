import { GLOBAL_OBJ } from "./worldwide.js";
import { isString } from "./is.js";
//#region node_modules/@sentry/core/build/esm/utils/browser.js
var WINDOW = GLOBAL_OBJ;
var DEFAULT_MAX_STRING_LENGTH = 80;
/**
* Given a child DOM element, returns a query-selector statement describing that
* and its ancestors
* e.g. [HTMLElement] => body > div > input#foo.btn[name=baz]
* @returns generated DOM path
*/
function htmlTreeAsString(elem, options = {}) {
	if (!elem) return "<unknown>";
	try {
		let currentElem = elem;
		const MAX_TRAVERSE_HEIGHT = 5;
		const out = [];
		let height = 0;
		let len = 0;
		const separator = " > ";
		const sepLength = 3;
		let nextStr;
		const keyAttrs = Array.isArray(options) ? options : options.keyAttrs;
		const maxStringLength = !Array.isArray(options) && options.maxStringLength || DEFAULT_MAX_STRING_LENGTH;
		while (currentElem && height++ < MAX_TRAVERSE_HEIGHT) {
			nextStr = _htmlElementAsString(currentElem, keyAttrs);
			if (nextStr === "html" || height > 1 && len + out.length * sepLength + nextStr.length >= maxStringLength) break;
			out.push(nextStr);
			len += nextStr.length;
			currentElem = currentElem.parentNode;
		}
		return out.reverse().join(separator);
	} catch {
		return "<unknown>";
	}
}
/**
* Returns a simple, query-selector representation of a DOM element
* e.g. [HTMLElement] => input#foo.btn[name=baz]
* @returns generated DOM path
*/
function _htmlElementAsString(el, keyAttrs) {
	const elem = el;
	const out = [];
	if (!elem?.tagName) return "";
	if (WINDOW.HTMLElement) {
		if (elem instanceof HTMLElement && elem.dataset) {
			if (elem.dataset["sentryComponent"]) return elem.dataset["sentryComponent"];
			if (elem.dataset["sentryElement"]) return elem.dataset["sentryElement"];
		}
	}
	out.push(elem.tagName.toLowerCase());
	const keyAttrPairs = keyAttrs?.length ? keyAttrs.filter((keyAttr) => elem.getAttribute(keyAttr)).map((keyAttr) => [keyAttr, elem.getAttribute(keyAttr)]) : null;
	if (keyAttrPairs?.length) keyAttrPairs.forEach((keyAttrPair) => {
		out.push(`[${keyAttrPair[0]}="${keyAttrPair[1]}"]`);
	});
	else {
		if (elem.id) out.push(`#${elem.id}`);
		const className = elem.className;
		if (className && isString(className)) {
			const classes = className.split(/\s+/);
			for (const c of classes) out.push(`.${c}`);
		}
	}
	for (const k of [
		"aria-label",
		"type",
		"name",
		"title",
		"alt"
	]) {
		const attr = elem.getAttribute(k);
		if (attr) out.push(`[${k}="${attr}"]`);
	}
	return out.join("");
}
/**
* A safe form of location.href
*/
function getLocationHref() {
	try {
		return WINDOW.document.location.href;
	} catch {
		return "";
	}
}
/**
* Given a DOM element, traverses up the tree until it finds the first ancestor node
* that has the `data-sentry-component` or `data-sentry-element` attribute with `data-sentry-component` taking
* precedence. This attribute is added at build-time by projects that have the component name annotation plugin installed.
*
* @returns a string representation of the component for the provided DOM element, or `null` if not found
*/
function getComponentName(elem, maxTraverseHeight = 5) {
	if (!WINDOW.HTMLElement) return null;
	let currentElem = elem;
	for (let i = 0; i < maxTraverseHeight; i++) {
		if (!currentElem) return null;
		if (currentElem instanceof HTMLElement) {
			if (currentElem.dataset["sentryComponent"]) return currentElem.dataset["sentryComponent"];
			if (currentElem.dataset["sentryElement"]) return currentElem.dataset["sentryElement"];
		}
		currentElem = currentElem.parentNode;
	}
	return null;
}
//#endregion
export { getComponentName, getLocationHref, htmlTreeAsString };
