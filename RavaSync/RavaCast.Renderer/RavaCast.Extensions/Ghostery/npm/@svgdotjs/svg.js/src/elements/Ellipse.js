import { registerMethods } from "../utils/methods.js";
import { proportionalSize } from "../utils/utils.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import SVGNumber from "../types/SVGNumber.js";
import Shape from "./Shape.js";
import { circled_exports } from "../modules/core/circled.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Ellipse.js
var Ellipse = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("ellipse", node), attrs);
	}
	size(width, height) {
		const p = proportionalSize(this, width, height);
		return this.rx(new SVGNumber(p.width).divide(2)).ry(new SVGNumber(p.height).divide(2));
	}
};
extend(Ellipse, circled_exports);
registerMethods("Container", { ellipse: wrapWithAttrCheck(function(width = 0, height = width) {
	return this.put(new Ellipse()).size(width, height).move(0, 0);
}) });
register(Ellipse, "Ellipse");
//#endregion
export { Ellipse as default };
