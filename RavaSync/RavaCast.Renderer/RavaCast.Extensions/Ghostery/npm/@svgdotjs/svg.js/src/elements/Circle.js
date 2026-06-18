import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import SVGNumber from "../types/SVGNumber.js";
import Shape from "./Shape.js";
import { cx, cy, height, width, x, y } from "../modules/core/circled.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Circle.js
var Circle = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("circle", node), attrs);
	}
	radius(r) {
		return this.attr("r", r);
	}
	rx(rx) {
		return this.attr("r", rx);
	}
	ry(ry) {
		return this.rx(ry);
	}
	size(size) {
		return this.radius(new SVGNumber(size).divide(2));
	}
};
extend(Circle, {
	x,
	y,
	cx,
	cy,
	width,
	height
});
registerMethods({ Container: { circle: wrapWithAttrCheck(function(size = 0) {
	return this.put(new Circle()).size(size).move(0, 0);
}) } });
register(Circle, "Circle");
//#endregion
export { Circle as default };
