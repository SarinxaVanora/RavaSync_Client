import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import SVGNumber from "../../types/SVGNumber.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/gradiented.js
var gradiented_exports = /* @__PURE__ */ __exportAll({
	from: () => from,
	to: () => to
});
function from(x, y) {
	return (this._element || this).type === "radialGradient" ? this.attr({
		fx: new SVGNumber(x),
		fy: new SVGNumber(y)
	}) : this.attr({
		x1: new SVGNumber(x),
		y1: new SVGNumber(y)
	});
}
function to(x, y) {
	return (this._element || this).type === "radialGradient" ? this.attr({
		cx: new SVGNumber(x),
		cy: new SVGNumber(y)
	}) : this.attr({
		x2: new SVGNumber(x),
		y2: new SVGNumber(y)
	});
}
//#endregion
export { from, gradiented_exports, to };
