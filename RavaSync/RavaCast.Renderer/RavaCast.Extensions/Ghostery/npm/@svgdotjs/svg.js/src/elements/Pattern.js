import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Box from "../types/Box.js";
import baseFind from "../modules/core/selector.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Pattern.js
var Pattern = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("pattern", node), attrs);
	}
	attr(a, b, c) {
		if (a === "transform") a = "patternTransform";
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
registerMethods({
	Container: { pattern(...args) {
		return this.defs().pattern(...args);
	} },
	Defs: { pattern: wrapWithAttrCheck(function(width, height, block) {
		return this.put(new Pattern()).update(block).attr({
			x: 0,
			y: 0,
			width,
			height,
			patternUnits: "userSpaceOnUse"
		});
	}) }
});
register(Pattern, "Pattern");
//#endregion
export { Pattern as default };
