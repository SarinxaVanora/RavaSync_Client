import { globals } from "../utils/window.js";
import { create, register } from "../utils/adopter.js";
import Dom from "./Dom.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Fragment.js
var Fragment = class extends Dom {
	constructor(node = globals.document.createDocumentFragment()) {
		super(node);
	}
	xml(xmlOrFn, outerXML, ns) {
		if (typeof xmlOrFn === "boolean") {
			ns = outerXML;
			outerXML = xmlOrFn;
			xmlOrFn = null;
		}
		if (xmlOrFn == null || typeof xmlOrFn === "function") {
			const wrapper = new Dom(create("wrapper", ns));
			wrapper.add(this.node.cloneNode(true));
			return wrapper.xml(false, ns);
		}
		return super.xml(xmlOrFn, false, ns);
	}
};
register(Fragment, "Fragment");
//#endregion
export { Fragment as default };
