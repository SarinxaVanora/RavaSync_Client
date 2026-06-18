import { SVG_NAMESPACE } from "./constants.js";
import { keys } from "./object.js";
import { CUSTOM_ELEMENTS, END, PREV, VALUE } from "./symbols.js";
import { knownBoundaries, knownSiblings } from "./utils.js";
import { attributeChangedCallback, connectedCallback } from "../interface/custom-element-registry.js";
import { esm_exports } from "../../../htmlparser2/dist/esm/index.js";
//#region node_modules/linkedom/esm/shared/parse-from-string.js
var { Parser } = esm_exports;
var notParsing = true;
var append = (self, node, active) => {
	const end = self[END];
	node.parentNode = self;
	knownBoundaries(end[PREV], node, end);
	if (active && node.nodeType === 1) connectedCallback(node);
	return node;
};
var attribute = (element, end, attribute, value, active) => {
	attribute[VALUE] = value;
	attribute.ownerElement = element;
	knownSiblings(end[PREV], attribute, end);
	if (attribute.name === "class") element.className = value;
	if (active) attributeChangedCallback(element, attribute.name, null, value);
};
var isNotParsing = () => notParsing;
var parseFromString = (document, isHTML, markupLanguage) => {
	const { active, registry } = document[CUSTOM_ELEMENTS];
	let node = document;
	let ownerSVGElement = null;
	let parsingCData = false;
	notParsing = false;
	const content = new Parser({
		onprocessinginstruction(name, data) {
			if (name.toLowerCase() === "!doctype") document.doctype = data.slice(name.length).trim();
		},
		onopentag(name, attributes) {
			let create = true;
			if (isHTML) {
				if (ownerSVGElement) {
					node = append(node, document.createElementNS(SVG_NAMESPACE, name), active);
					node.ownerSVGElement = ownerSVGElement;
					create = false;
				} else if (name === "svg" || name === "SVG") {
					ownerSVGElement = document.createElementNS(SVG_NAMESPACE, name);
					node = append(node, ownerSVGElement, active);
					create = false;
				} else if (active) {
					const ce = name.includes("-") ? name : attributes.is || "";
					if (ce && registry.has(ce)) {
						const { Class } = registry.get(ce);
						node = append(node, new Class(), active);
						delete attributes.is;
						create = false;
					}
				}
			}
			if (create) node = append(node, document.createElement(name), false);
			let end = node[END];
			for (const name of keys(attributes)) attribute(node, end, document.createAttribute(name), attributes[name], active);
		},
		oncomment(data) {
			append(node, document.createComment(data), active);
		},
		ontext(text) {
			if (parsingCData) append(node, document.createCDATASection(text), active);
			else append(node, document.createTextNode(text), active);
		},
		oncdatastart() {
			parsingCData = true;
		},
		oncdataend() {
			parsingCData = false;
		},
		onclosetag() {
			if (isHTML && node === ownerSVGElement) ownerSVGElement = null;
			node = node.parentNode;
		}
	}, {
		lowerCaseAttributeNames: false,
		decodeEntities: true,
		xmlMode: !isHTML
	});
	content.write(markupLanguage);
	content.end();
	notParsing = true;
	return document;
};
//#endregion
export { isNotParsing, parseFromString };
