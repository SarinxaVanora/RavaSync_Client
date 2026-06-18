import { registerMethods } from "../../utils/methods.js";
import Color from "../../types/Color.js";
import Point from "../../types/Point.js";
import Matrix from "../../types/Matrix.js";
import SVGNumber from "../../types/SVGNumber.js";
import Element from "../../elements/Element.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/sugar.js
var sugar = {
	stroke: [
		"color",
		"width",
		"opacity",
		"linecap",
		"linejoin",
		"miterlimit",
		"dasharray",
		"dashoffset"
	],
	fill: [
		"color",
		"opacity",
		"rule"
	],
	prefix: function(t, a) {
		return a === "color" ? t : t + "-" + a;
	}
};
["fill", "stroke"].forEach(function(m) {
	const extension = {};
	let i;
	extension[m] = function(o) {
		if (typeof o === "undefined") return this.attr(m);
		if (typeof o === "string" || o instanceof Color || Color.isRgb(o) || o instanceof Element) this.attr(m, o);
		else for (i = sugar[m].length - 1; i >= 0; i--) if (o[sugar[m][i]] != null) this.attr(sugar.prefix(m, sugar[m][i]), o[sugar[m][i]]);
		return this;
	};
	registerMethods(["Element", "Runner"], extension);
});
registerMethods(["Element", "Runner"], {
	matrix: function(mat, b, c, d, e, f) {
		if (mat == null) return new Matrix(this);
		return this.attr("transform", new Matrix(mat, b, c, d, e, f));
	},
	rotate: function(angle, cx, cy) {
		return this.transform({
			rotate: angle,
			ox: cx,
			oy: cy
		}, true);
	},
	skew: function(x, y, cx, cy) {
		return arguments.length === 1 || arguments.length === 3 ? this.transform({
			skew: x,
			ox: y,
			oy: cx
		}, true) : this.transform({
			skew: [x, y],
			ox: cx,
			oy: cy
		}, true);
	},
	shear: function(lam, cx, cy) {
		return this.transform({
			shear: lam,
			ox: cx,
			oy: cy
		}, true);
	},
	scale: function(x, y, cx, cy) {
		return arguments.length === 1 || arguments.length === 3 ? this.transform({
			scale: x,
			ox: y,
			oy: cx
		}, true) : this.transform({
			scale: [x, y],
			ox: cx,
			oy: cy
		}, true);
	},
	translate: function(x, y) {
		return this.transform({ translate: [x, y] }, true);
	},
	relative: function(x, y) {
		return this.transform({ relative: [x, y] }, true);
	},
	flip: function(direction = "both", origin = "center") {
		if ("xybothtrue".indexOf(direction) === -1) {
			origin = direction;
			direction = "both";
		}
		return this.transform({
			flip: direction,
			origin
		}, true);
	},
	opacity: function(value) {
		return this.attr("opacity", value);
	}
});
registerMethods("radius", { radius: function(x, y = x) {
	return (this._element || this).type === "radialGradient" ? this.attr("r", new SVGNumber(x)) : this.rx(x).ry(y);
} });
registerMethods("Path", {
	length: function() {
		return this.node.getTotalLength();
	},
	pointAt: function(length) {
		return new Point(this.node.getPointAtLength(length));
	}
});
registerMethods(["Element", "Runner"], { font: function(a, v) {
	if (typeof a === "object") {
		for (v in a) this.font(v, a[v]);
		return this;
	}
	return a === "leading" ? this.leading(v) : a === "anchor" ? this.attr("text-anchor", v) : a === "size" || a === "family" || a === "weight" || a === "stretch" || a === "variant" || a === "style" ? this.attr("font-" + a, v) : this.attr(a, v);
} });
registerMethods("Element", [
	"click",
	"dblclick",
	"mousedown",
	"mouseup",
	"mouseover",
	"mouseout",
	"mousemove",
	"mouseenter",
	"mouseleave",
	"touchstart",
	"touchmove",
	"touchleave",
	"touchend",
	"touchcancel",
	"contextmenu",
	"wheel",
	"pointerdown",
	"pointermove",
	"pointerup",
	"pointerleave",
	"pointercancel"
].reduce(function(last, event) {
	const fn = function(f) {
		if (f === null) this.off(event);
		else this.on(event, f);
		return this;
	};
	last[event] = fn;
	return last;
}, {}));
//#endregion
