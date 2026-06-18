import { registerMethods } from "../utils/methods.js";
import { globals } from "../utils/window.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import SVGNumber from "../types/SVGNumber.js";
import Shape from "./Shape.js";
import { textable_exports } from "../modules/core/textable.js";
import Text from "./Text.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Tspan.js
var Tspan = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("tspan", node), attrs);
		this._build = false;
	}
	dx(dx) {
		return this.attr("dx", dx);
	}
	dy(dy) {
		return this.attr("dy", dy);
	}
	newLine() {
		this.dom.newLined = true;
		const text = this.parent();
		if (!(text instanceof Text)) return this;
		const i = text.index(this);
		const fontSize = globals.window.getComputedStyle(this.node).getPropertyValue("font-size");
		const dy = text.dom.leading * new SVGNumber(fontSize);
		return this.dy(i ? dy : 0).attr("x", text.x());
	}
	text(text) {
		if (text == null) return this.node.textContent + (this.dom.newLined ? "\n" : "");
		if (typeof text === "function") {
			this.clear().build(true);
			text.call(this, this);
			this.build(false);
		} else this.plain(text);
		return this;
	}
};
extend(Tspan, textable_exports);
registerMethods({
	Tspan: { tspan: wrapWithAttrCheck(function(text = "") {
		const tspan = new Tspan();
		if (!this._build) this.clear();
		return this.put(tspan).text(text);
	}) },
	Text: { newLine: function(text = "") {
		return this.tspan(text).newLine();
	} }
});
register(Tspan, "Tspan");
//#endregion
export { Tspan as default };
