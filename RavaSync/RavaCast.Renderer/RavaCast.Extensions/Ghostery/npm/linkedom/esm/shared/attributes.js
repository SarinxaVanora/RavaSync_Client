import { CLASS_LIST, NEXT, PREV, VALUE } from "./symbols.js";
import { knownAdjacent, knownSiblings } from "./utils.js";
import { attributeChangedCallback } from "../interface/custom-element-registry.js";
import { attributeChangedCallback as attributeChangedCallback$1 } from "../interface/mutation-observer.js";
//#region node_modules/linkedom/esm/shared/attributes.js
var emptyAttributes = new Set([
	"allowfullscreen",
	"allowpaymentrequest",
	"async",
	"autofocus",
	"autoplay",
	"checked",
	"class",
	"contenteditable",
	"controls",
	"default",
	"defer",
	"disabled",
	"draggable",
	"formnovalidate",
	"hidden",
	"id",
	"ismap",
	"itemscope",
	"loop",
	"multiple",
	"muted",
	"nomodule",
	"novalidate",
	"open",
	"playsinline",
	"readonly",
	"required",
	"reversed",
	"selected",
	"style",
	"truespeed"
]);
var setAttribute = (element, attribute) => {
	const { [VALUE]: value, name } = attribute;
	attribute.ownerElement = element;
	knownSiblings(element, attribute, element[NEXT]);
	if (name === "class") element.className = value;
	attributeChangedCallback$1(element, name, null);
	attributeChangedCallback(element, name, null, value);
};
var removeAttribute = (element, attribute) => {
	const { [VALUE]: value, name } = attribute;
	knownAdjacent(attribute[PREV], attribute[NEXT]);
	attribute.ownerElement = attribute[PREV] = attribute[NEXT] = null;
	if (name === "class") element[CLASS_LIST] = null;
	attributeChangedCallback$1(element, name, value);
	attributeChangedCallback(element, name, value, null);
};
var booleanAttribute = {
	get(element, name) {
		return element.hasAttribute(name);
	},
	set(element, name, value) {
		if (value) element.setAttribute(name, "");
		else element.removeAttribute(name);
	}
};
var numericAttribute = {
	get(element, name) {
		return parseFloat(element.getAttribute(name) || 0);
	},
	set(element, name, value) {
		element.setAttribute(name, value);
	}
};
var stringAttribute = {
	get(element, name) {
		return element.getAttribute(name) || "";
	},
	set(element, name, value) {
		element.setAttribute(name, value);
	}
};
//#endregion
export { booleanAttribute, emptyAttributes, numericAttribute, removeAttribute, setAttribute, stringAttribute };
