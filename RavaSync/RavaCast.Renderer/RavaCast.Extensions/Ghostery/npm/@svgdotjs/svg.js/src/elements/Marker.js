import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Marker.js
var Marker = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("marker", node), attrs);
	}
	height(height) {
		return this.attr("markerHeight", height);
	}
	orient(orient) {
		return this.attr("orient", orient);
	}
	ref(x, y) {
		return this.attr("refX", x).attr("refY", y);
	}
	toString() {
		return "url(#" + this.id() + ")";
	}
	update(block) {
		this.clear();
		if (typeof block === "function") block.call(this, this);
		return this;
	}
	width(width) {
		return this.attr("markerWidth", width);
	}
};
registerMethods({
	Container: { marker(...args) {
		return this.defs().marker(...args);
	} },
	Defs: { marker: wrapWithAttrCheck(function(width, height, block) {
		return this.put(new Marker()).size(width, height).ref(width / 2, height / 2).viewbox(0, 0, width, height).attr("orient", "auto").update(block);
	}) },
	marker: { marker(marker, width, height, block) {
		let attr = ["marker"];
		if (marker !== "all") attr.push(marker);
		attr = attr.join("-");
		marker = arguments[1] instanceof Marker ? arguments[1] : this.defs().marker(width, height, block);
		return this.attr(attr, marker);
	} }
});
register(Marker, "Marker");
//#endregion
export { Marker as default };
