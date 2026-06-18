import { getPlaceholder } from "./utils.js";
import { compileTemplate } from "./core.js";
import { helpers_exports } from "./helpers/index.js";
import { methods_exports } from "./methods.js";
//#region node_modules/hybrids/src/template/index.js
var PLACEHOLDER = getPlaceholder();
var PLACEHOLDER_SVG = getPlaceholder("svg");
var PLACEHOLDER_LAYOUT = getPlaceholder("layout");
var templates = /* @__PURE__ */ new Map();
function compile(parts, args, id, isSVG, isMsg) {
	function fn(host, target) {
		if (fn.useLayout) id += PLACEHOLDER_LAYOUT;
		let render = templates.get(id);
		if (!render) {
			render = compileTemplate(parts, isSVG, isMsg, fn.useLayout);
			templates.set(id, render);
		}
		if (fn.plugins) return fn.plugins.reduce((acc, plugin) => plugin(acc), () => render(host, target, args, fn.styleSheets))(host, target);
		else return render(host, target, args, fn.styleSheets);
	}
	return Object.assign(fn, methods_exports);
}
function html(parts, ...args) {
	return compile(parts, args, parts.join(PLACEHOLDER), false, false);
}
function svg(parts, ...args) {
	return compile(parts, args, parts.join(PLACEHOLDER) + PLACEHOLDER_SVG, true, false);
}
Object.freeze(Object.assign(html, helpers_exports));
//#endregion
export { compile, html, svg };
