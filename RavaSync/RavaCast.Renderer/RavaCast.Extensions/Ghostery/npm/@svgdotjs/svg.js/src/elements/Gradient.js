import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Box from "../types/Box.js";
import baseFind from "../modules/core/selector.js";
import Container from "./Container.js";
import { gradiented_exports } from "../modules/core/gradiented.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Gradient.js
var Gradient = class extends Container {
	constructor(type, attrs) {
		super(nodeOrNew(type + "Gradient", typeof type === "string" ? null : type), attrs);
	}
	attr(a, b, c) {
		if (a === "transform") a = "gradientTransform";
		return super.attr(a, b, c);
	}
	bbox() {
		return new Box();
	}
	targets() {
		return baseFind("svg [fill*=" + this.id() + "]");
	}
	toString() {
		return this.url();
	}
	update(block) {
		this.clear();
		if (typeof block === "function") block.call(this, this);
		return this;
	}
	url() {
		return "url(#" + this.id() + ")";
	}
};
extend(Gradient, gradiented_exports);
registerMethods({
	Container: { gradient(...args) {
		return this.defs().gradient(...args);
	} },
	Defs: { gradient: wrapWithAttrCheck(function(type, block) {
		return this.put(new Gradient(type)).update(block);
	}) }
});
register(Gradient, "Gradient");
//#endregion
export { Gradient as default };
