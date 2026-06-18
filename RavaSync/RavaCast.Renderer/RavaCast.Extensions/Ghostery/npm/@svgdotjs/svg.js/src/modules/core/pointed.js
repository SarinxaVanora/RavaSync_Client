import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import PointArray from "../../types/PointArray.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/pointed.js
var pointed_exports = /* @__PURE__ */ __exportAll({
	MorphArray: () => MorphArray,
	height: () => height,
	width: () => width,
	x: () => x,
	y: () => y
});
var MorphArray = PointArray;
function x(x) {
	return x == null ? this.bbox().x : this.move(x, this.bbox().y);
}
function y(y) {
	return y == null ? this.bbox().y : this.move(this.bbox().x, y);
}
function width(width) {
	const b = this.bbox();
	return width == null ? b.width : this.size(width, b.height);
}
function height(height) {
	const b = this.bbox();
	return height == null ? b.height : this.size(b.width, height);
}
//#endregion
export { pointed_exports };
