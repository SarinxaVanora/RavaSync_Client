import { VALUE } from "../shared/symbols.js";
import { escape } from "../shared/text-escaper.js";
import { CharacterData } from "./character-data.js";
//#region node_modules/linkedom/esm/interface/text.js
/**
* @implements globalThis.Text
*/
var Text = class Text extends CharacterData {
	constructor(ownerDocument, data = "") {
		super(ownerDocument, "#text", 3, data);
	}
	get wholeText() {
		const text = [];
		let { previousSibling, nextSibling } = this;
		while (previousSibling) {
			if (previousSibling.nodeType === 3) text.unshift(previousSibling[VALUE]);
			else break;
			previousSibling = previousSibling.previousSibling;
		}
		text.push(this[VALUE]);
		while (nextSibling) {
			if (nextSibling.nodeType === 3) text.push(nextSibling[VALUE]);
			else break;
			nextSibling = nextSibling.nextSibling;
		}
		return text.join("");
	}
	cloneNode() {
		const { ownerDocument, [VALUE]: data } = this;
		return new Text(ownerDocument, data);
	}
	toString() {
		return escape(this[VALUE]);
	}
};
//#endregion
export { Text };
