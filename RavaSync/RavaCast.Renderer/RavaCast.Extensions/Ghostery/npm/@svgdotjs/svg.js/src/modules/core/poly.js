import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import { proportionalSize } from "../../utils/utils.js";
import PointArray from "../../types/PointArray.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/poly.js
var poly_exports = /* @__PURE__ */ __exportAll({
	array: () => array,
	clear: () => clear,
	move: () => move,
	plot: () => plot,
	size: () => size
});
function array() {
	return this._array || (this._array = new PointArray(this.attr("points")));
}
function clear() {
	delete this._array;
	return this;
}
function move(x, y) {
	return this.attr("points", this.array().move(x, y));
}
function plot(p) {
	return p == null ? this.array() : this.clear().attr("points", typeof p === "string" ? p : this._array = new PointArray(p));
}
function size(width, height) {
	const p = proportionalSize(this, width, height);
	return this.attr("points", this.array().size(p.width, p.height));
}
//#endregion
export { poly_exports };
