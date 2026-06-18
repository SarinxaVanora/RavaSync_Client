import { isNumber } from "./regex.js";
import Color from "../../types/Color.js";
import { attrs } from "./defaults.js";
import SVGArray from "../../types/SVGArray.js";
import SVGNumber from "../../types/SVGNumber.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/attr.js
var colorAttributes = new Set([
	"fill",
	"stroke",
	"color",
	"bgcolor",
	"stop-color",
	"flood-color",
	"lighting-color"
]);
var hooks = [];
function registerAttrHook(fn) {
	hooks.push(fn);
}
function attr(attr, val, ns) {
	if (attr == null) {
		attr = {};
		val = this.node.attributes;
		for (const node of val) attr[node.nodeName] = isNumber.test(node.nodeValue) ? parseFloat(node.nodeValue) : node.nodeValue;
		return attr;
	} else if (attr instanceof Array) return attr.reduce((last, curr) => {
		last[curr] = this.attr(curr);
		return last;
	}, {});
	else if (typeof attr === "object" && attr.constructor === Object) for (val in attr) this.attr(val, attr[val]);
	else if (val === null) this.node.removeAttribute(attr);
	else if (val == null) {
		val = this.node.getAttribute(attr);
		return val == null ? attrs[attr] : isNumber.test(val) ? parseFloat(val) : val;
	} else {
		val = hooks.reduce((_val, hook) => {
			return hook(attr, _val, this);
		}, val);
		if (typeof val === "number") val = new SVGNumber(val);
		else if (colorAttributes.has(attr) && Color.isColor(val)) val = new Color(val);
		else if (val.constructor === Array) val = new SVGArray(val);
		if (attr === "leading") {
			if (this.leading) this.leading(val);
		} else typeof ns === "string" ? this.node.setAttributeNS(ns, attr, val.toString()) : this.node.setAttribute(attr, val.toString());
		if (this.rebuild && (attr === "font-size" || attr === "x")) this.rebuild();
	}
	return this;
}
//#endregion
export { attr as default, registerAttrHook };
