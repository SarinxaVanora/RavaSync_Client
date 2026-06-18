import { registerMethods } from "../utils/methods.js";
import { proportionalSize } from "../utils/utils.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
import PointArray from "../types/PointArray.js";
import { pointed_exports } from "../modules/core/pointed.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Line.js
var Line = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("line", node), attrs);
	}
	array() {
		return new PointArray([[this.attr("x1"), this.attr("y1")], [this.attr("x2"), this.attr("y2")]]);
	}
	move(x, y) {
		return this.attr(this.array().move(x, y).toLine());
	}
	plot(x1, y1, x2, y2) {
		if (x1 == null) return this.array();
		else if (typeof y1 !== "undefined") x1 = {
			x1,
			y1,
			x2,
			y2
		};
		else x1 = new PointArray(x1).toLine();
		return this.attr(x1);
	}
	size(width, height) {
		const p = proportionalSize(this, width, height);
		return this.attr(this.array().size(p.width, p.height).toLine());
	}
};
extend(Line, pointed_exports);
registerMethods({ Container: { line: wrapWithAttrCheck(function(...args) {
	return Line.prototype.plot.apply(this.put(new Line()), args[0] != null ? args : [
		0,
		0,
		0,
		0
	]);
}) } });
register(Line, "Line");
//#endregion
export { Line as default };
