import { registerMethods } from "../../utils/methods.js";
import { isBlank } from "../core/regex.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/css.js
function css(style, val) {
	const ret = {};
	if (arguments.length === 0) {
		this.node.style.cssText.split(/\s*;\s*/).filter(function(el) {
			return !!el.length;
		}).forEach(function(el) {
			const t = el.split(/\s*:\s*/);
			ret[t[0]] = t[1];
		});
		return ret;
	}
	if (arguments.length < 2) {
		if (Array.isArray(style)) {
			for (const name of style) {
				const cased = name;
				ret[name] = this.node.style.getPropertyValue(cased);
			}
			return ret;
		}
		if (typeof style === "string") return this.node.style.getPropertyValue(style);
		if (typeof style === "object") for (const name in style) this.node.style.setProperty(name, style[name] == null || isBlank.test(style[name]) ? "" : style[name]);
	}
	if (arguments.length === 2) this.node.style.setProperty(style, val == null || isBlank.test(val) ? "" : val);
	return this;
}
function show() {
	return this.css("display", "");
}
function hide() {
	return this.css("display", "none");
}
function visible() {
	return this.css("display") !== "none";
}
registerMethods("Dom", {
	css,
	show,
	hide,
	visible
});
//#endregion
