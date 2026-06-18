import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import SVGNumber from "../../types/SVGNumber.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/circled.js
var circled_exports = /* @__PURE__ */ __exportAll({
	cx: () => cx,
	cy: () => cy,
	height: () => height,
	rx: () => rx,
	ry: () => ry,
	width: () => width,
	x: () => x,
	y: () => y
});
function rx(rx) {
	return this.attr("rx", rx);
}
function ry(ry) {
	return this.attr("ry", ry);
}
function x(x) {
	return x == null ? this.cx() - this.rx() : this.cx(x + this.rx());
}
function y(y) {
	return y == null ? this.cy() - this.ry() : this.cy(y + this.ry());
}
function cx(x) {
	return this.attr("cx", x);
}
function cy(y) {
	return this.attr("cy", y);
}
function width(width) {
	return width == null ? this.rx() * 2 : this.rx(new SVGNumber(width).divide(2));
}
function height(height) {
	return height == null ? this.ry() * 2 : this.ry(new SVGNumber(height).divide(2));
}
//#endregion
export { circled_exports, cx, cy, height, rx, ry, width, x, y };
