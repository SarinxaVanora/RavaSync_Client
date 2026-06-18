import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
import PointArray from "../types/PointArray.js";
import { pointed_exports } from "../modules/core/pointed.js";
import { poly_exports } from "../modules/core/poly.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Polyline.js
var Polyline = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("polyline", node), attrs);
	}
};
registerMethods({ Container: { polyline: wrapWithAttrCheck(function(p) {
	return this.put(new Polyline()).plot(p || new PointArray());
}) } });
extend(Polyline, pointed_exports);
extend(Polyline, poly_exports);
register(Polyline, "Polyline");
//#endregion
export { Polyline as default };
