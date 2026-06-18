import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import baseFind from "../modules/core/selector.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/ClipPath.js
var ClipPath = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("clipPath", node), attrs);
	}
	remove() {
		this.targets().forEach(function(el) {
			el.unclip();
		});
		return super.remove();
	}
	targets() {
		return baseFind("svg [clip-path*=" + this.id() + "]");
	}
};
registerMethods({
	Container: { clip: wrapWithAttrCheck(function() {
		return this.defs().put(new ClipPath());
	}) },
	Element: {
		clipper() {
			return this.reference("clip-path");
		},
		clipWith(element) {
			const clipper = element instanceof ClipPath ? element : this.parent().clip().add(element);
			return this.attr("clip-path", "url(#" + clipper.id() + ")");
		},
		unclip() {
			return this.attr("clip-path", null);
		}
	}
});
register(ClipPath, "ClipPath");
//#endregion
export { ClipPath as default };
