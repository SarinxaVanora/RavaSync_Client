import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
import PointArray from "../types/PointArray.js";
import { pointed_exports } from "../modules/core/pointed.js";
import { poly_exports } from "../modules/core/poly.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Polygon.js
var Polygon = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("polygon", node), attrs);
	}
};
registerMethods({ Container: { polygon: wrapWithAttrCheck(function(p) {
	return this.put(new Polygon()).plot(p || new PointArray());
}) } });
extend(Polygon, pointed_exports);
extend(Polygon, poly_exports);
register(Polygon, "Polygon");
//#endregion
export { Polygon as default };
