import { $String } from "../shared/utils.js";
import { Element } from "../interface/element.js";
//#region node_modules/linkedom/esm/svg/element.js
var classNames = /* @__PURE__ */ new WeakMap();
var handler = {
	get(target, name) {
		return target[name];
	},
	set(target, name, value) {
		target[name] = value;
		return true;
	}
};
/**
* @implements globalThis.SVGElement
*/
var SVGElement = class extends Element {
	constructor(ownerDocument, localName, ownerSVGElement = null) {
		super(ownerDocument, localName);
		this.ownerSVGElement = ownerSVGElement;
	}
	get className() {
		if (!classNames.has(this)) classNames.set(this, new Proxy({
			baseVal: "",
			animVal: ""
		}, handler));
		return classNames.get(this);
	}
	/* c8 ignore start */
	set className(value) {
		const { classList } = this;
		classList.clear();
		classList.add(...$String(value).split(/\s+/));
	}
	/* c8 ignore stop */
	get namespaceURI() {
		return "http://www.w3.org/2000/svg";
	}
	getAttribute(name) {
		return name === "class" ? [...this.classList].join(" ") : super.getAttribute(name);
	}
	setAttribute(name, value) {
		if (name === "class") this.className = value;
		else if (name === "style") {
			const { className } = this;
			className.baseVal = className.animVal = value;
		}
		super.setAttribute(name, value);
	}
};
//#endregion
export { SVGElement };
