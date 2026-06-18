import { END, NEXT, VALUE } from "./symbols.js";
import { getEnd } from "./utils.js";
//#region node_modules/linkedom/esm/shared/jsdon.js
var loopSegment = ({ [NEXT]: next, [END]: end }, json) => {
	while (next !== end) {
		switch (next.nodeType) {
			case 2:
				attrAsJSON(next, json);
				break;
			case 3:
			case 8:
			case 4:
				characterDataAsJSON(next, json);
				break;
			case 1:
				elementAsJSON(next, json);
				next = getEnd(next);
				break;
			case 10:
				documentTypeAsJSON(next, json);
				break;
		}
		next = next[NEXT];
	}
	const last = json.length - 1;
	const value = json[last];
	if (typeof value === "number" && value < 0) json[last] += -1;
	else json.push(-1);
};
var attrAsJSON = (attr, json) => {
	json.push(2, attr.name);
	const value = attr[VALUE].trim();
	if (value) json.push(value);
};
var characterDataAsJSON = (node, json) => {
	const value = node[VALUE];
	if (value.trim()) json.push(node.nodeType, value);
};
var nonElementAsJSON = (node, json) => {
	json.push(node.nodeType);
	loopSegment(node, json);
};
var documentTypeAsJSON = ({ name, publicId, systemId }, json) => {
	json.push(10, name);
	if (publicId) json.push(publicId);
	if (systemId) json.push(systemId);
};
var elementAsJSON = (element, json) => {
	json.push(1, element.localName);
	loopSegment(element, json);
};
//#endregion
export { attrAsJSON, characterDataAsJSON, documentTypeAsJSON, elementAsJSON, nonElementAsJSON };
