import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
import { rx, ry } from "../modules/core/circled.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Rect.js
var Rect = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("rect", node), attrs);
	}
};
extend(Rect, {
	rx,
	ry
});
registerMethods({ Container: { rect: wrapWithAttrCheck(function(width, height) {
	return this.put(new Rect()).size(width, height);
}) } });
register(Rect, "Rect");
//#endregion
export { Rect as default };
