import { registerMethods } from "../utils/methods.js";
import { proportionalSize } from "../utils/utils.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
import PathArray from "../types/PathArray.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Path.js
var Path = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("path", node), attrs);
	}
	array() {
		return this._array || (this._array = new PathArray(this.attr("d")));
	}
	clear() {
		delete this._array;
		return this;
	}
	height(height) {
		return height == null ? this.bbox().height : this.size(this.bbox().width, height);
	}
	move(x, y) {
		return this.attr("d", this.array().move(x, y));
	}
	plot(d) {
		return d == null ? this.array() : this.clear().attr("d", typeof d === "string" ? d : this._array = new PathArray(d));
	}
	size(width, height) {
		const p = proportionalSize(this, width, height);
		return this.attr("d", this.array().size(p.width, p.height));
	}
	width(width) {
		return width == null ? this.bbox().width : this.size(width, this.bbox().height);
	}
	x(x) {
		return x == null ? this.bbox().x : this.move(x, this.bbox().y);
	}
	y(y) {
		return y == null ? this.bbox().y : this.move(this.bbox().x, y);
	}
};
Path.prototype.MorphArray = PathArray;
registerMethods({ Container: { path: wrapWithAttrCheck(function(d) {
	return this.put(new Path()).plot(d || new PathArray());
}) } });
register(Path, "Path");
//#endregion
export { Path as default };
