import { SHEET } from "../shared/symbols.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { TextElement } from "./text-element.js";
import { require_lib } from "../../../cssom/lib/index.js";
//#region node_modules/linkedom/esm/html/style-element.js
var import_lib = require_lib();
var tagName = "style";
/**
* @implements globalThis.HTMLStyleElement
*/
var HTMLStyleElement = class extends TextElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
		this[SHEET] = null;
	}
	get sheet() {
		const sheet = this[SHEET];
		if (sheet !== null) return sheet;
		return this[SHEET] = (0, import_lib.parse)(this.textContent);
	}
	get innerHTML() {
		return super.innerHTML || "";
	}
	set innerHTML(value) {
		super.textContent = value;
		this[SHEET] = null;
	}
	get innerText() {
		return super.innerText || "";
	}
	set innerText(value) {
		super.textContent = value;
		this[SHEET] = null;
	}
	get textContent() {
		return super.textContent || "";
	}
	set textContent(value) {
		super.textContent = value;
		this[SHEET] = null;
	}
};
registerHTMLClass(tagName, HTMLStyleElement);
//#endregion
export { HTMLStyleElement };
