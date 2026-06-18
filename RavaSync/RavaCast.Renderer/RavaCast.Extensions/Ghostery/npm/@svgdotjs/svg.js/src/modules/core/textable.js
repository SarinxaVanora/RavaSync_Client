import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import { globals } from "../../utils/window.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/textable.js
var textable_exports = /* @__PURE__ */ __exportAll({
	amove: () => amove,
	ax: () => ax,
	ay: () => ay,
	build: () => build,
	center: () => center,
	cx: () => cx,
	cy: () => cy,
	length: () => length,
	move: () => move,
	plain: () => plain,
	x: () => x,
	y: () => y
});
function plain(text) {
	if (this._build === false) this.clear();
	this.node.appendChild(globals.document.createTextNode(text));
	return this;
}
function length() {
	return this.node.getComputedTextLength();
}
function x(x, box = this.bbox()) {
	if (x == null) return box.x;
	return this.attr("x", this.attr("x") + x - box.x);
}
function y(y, box = this.bbox()) {
	if (y == null) return box.y;
	return this.attr("y", this.attr("y") + y - box.y);
}
function move(x, y, box = this.bbox()) {
	return this.x(x, box).y(y, box);
}
function cx(x, box = this.bbox()) {
	if (x == null) return box.cx;
	return this.attr("x", this.attr("x") + x - box.cx);
}
function cy(y, box = this.bbox()) {
	if (y == null) return box.cy;
	return this.attr("y", this.attr("y") + y - box.cy);
}
function center(x, y, box = this.bbox()) {
	return this.cx(x, box).cy(y, box);
}
function ax(x) {
	return this.attr("x", x);
}
function ay(y) {
	return this.attr("y", y);
}
function amove(x, y) {
	return this.ax(x).ay(y);
}
function build(build) {
	this._build = !!build;
	return this;
}
//#endregion
export { textable_exports };
