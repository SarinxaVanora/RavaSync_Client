import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register } from "../utils/adopter.js";
import SVGNumber from "../types/SVGNumber.js";
import Element from "./Element.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Stop.js
var Stop = class extends Element {
	constructor(node, attrs = node) {
		super(nodeOrNew("stop", node), attrs);
	}
	update(o) {
		if (typeof o === "number" || o instanceof SVGNumber) o = {
			offset: arguments[0],
			color: arguments[1],
			opacity: arguments[2]
		};
		if (o.opacity != null) this.attr("stop-opacity", o.opacity);
		if (o.color != null) this.attr("stop-color", o.color);
		if (o.offset != null) this.attr("offset", new SVGNumber(o.offset));
		return this;
	}
};
registerMethods({ Gradient: { stop: function(offset, color, opacity) {
	return this.put(new Stop()).update(offset, color, opacity);
} } });
register(Stop, "Stop");
//#endregion
export { Stop as default };
