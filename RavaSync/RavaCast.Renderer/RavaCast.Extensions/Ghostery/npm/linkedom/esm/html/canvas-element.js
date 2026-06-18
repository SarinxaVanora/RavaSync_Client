import { __toESM } from "../../../../virtual/_rolldown/runtime.js";
import { IMAGE } from "../shared/symbols.js";
import { numericAttribute } from "../shared/attributes.js";
import { registerHTMLClass } from "../shared/register-html-class.js";
import { HTMLElement } from "./element.js";
import { require_canvas } from "../../commonjs/canvas.js";
var { createCanvas } = (/* @__PURE__ */ __toESM(require_canvas(), 1)).default;
var tagName = "canvas";
/**
* @implements globalThis.HTMLCanvasElement
*/
var HTMLCanvasElement = class extends HTMLElement {
	constructor(ownerDocument, localName = tagName) {
		super(ownerDocument, localName);
		this[IMAGE] = createCanvas(300, 150);
	}
	get width() {
		return this[IMAGE].width;
	}
	set width(value) {
		numericAttribute.set(this, "width", value);
		this[IMAGE].width = value;
	}
	get height() {
		return this[IMAGE].height;
	}
	set height(value) {
		numericAttribute.set(this, "height", value);
		this[IMAGE].height = value;
	}
	getContext(type) {
		return this[IMAGE].getContext(type);
	}
	toDataURL(...args) {
		return this[IMAGE].toDataURL(...args);
	}
};
registerHTMLClass(tagName, HTMLCanvasElement);
//#endregion
export { HTMLCanvasElement };
