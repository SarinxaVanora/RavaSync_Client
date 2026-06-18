import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/text-element.js
var { toString } = HTMLElement.prototype;
var TextElement = class extends HTMLElement {
	get innerHTML() {
		return this.textContent;
	}
	set innerHTML(html) {
		this.textContent = html;
	}
	toString() {
		return toString.call(this.cloneNode()).replace("><", () => `>${this.textContent}<`);
	}
};
//#endregion
export { TextElement };
