import { registerMethods } from "../utils/methods.js";
import { svg, xlink, xmlns } from "../modules/core/namespaces.js";
import { globals } from "../utils/window.js";
import { adopt, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Container from "./Container.js";
import Defs from "./Defs.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Svg.js
var Svg = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("svg", node), attrs);
		this.namespace();
	}
	defs() {
		if (!this.isRoot()) return this.root().defs();
		return adopt(this.node.querySelector("defs")) || this.put(new Defs());
	}
	isRoot() {
		return !this.node.parentNode || !(this.node.parentNode instanceof globals.window.SVGElement) && this.node.parentNode.nodeName !== "#document-fragment";
	}
	namespace() {
		if (!this.isRoot()) return this.root().namespace();
		return this.attr({
			xmlns: svg,
			version: "1.1"
		}).attr("xmlns:xlink", xlink, xmlns);
	}
	removeNamespace() {
		return this.attr({
			xmlns: null,
			version: null
		}).attr("xmlns:xlink", null, xmlns).attr("xmlns:svgjs", null, xmlns);
	}
	root() {
		if (this.isRoot()) return this;
		return super.root();
	}
};
registerMethods({ Container: { nested: wrapWithAttrCheck(function() {
	return this.put(new Svg());
}) } });
register(Svg, "Svg", true);
//#endregion
export { Svg as default };
