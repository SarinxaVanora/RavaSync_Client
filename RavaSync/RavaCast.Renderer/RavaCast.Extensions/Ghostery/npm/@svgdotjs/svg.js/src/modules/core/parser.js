import { globals } from "../../utils/window.js";
import { makeInstance } from "../../utils/adopter.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/parser.js
function parser() {
	if (!parser.nodes) {
		const svg = makeInstance().size(2, 0);
		svg.node.style.cssText = [
			"opacity: 0",
			"position: absolute",
			"left: -100%",
			"top: -100%",
			"overflow: hidden"
		].join(";");
		svg.attr("focusable", "false");
		svg.attr("aria-hidden", "true");
		parser.nodes = {
			svg,
			path: svg.path().node
		};
	}
	if (!parser.nodes.svg.node.parentNode) {
		const b = globals.document.body || globals.document.documentElement;
		parser.nodes.svg.addTo(b);
	}
	return parser.nodes;
}
//#endregion
export { parser as default };
