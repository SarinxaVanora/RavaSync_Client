import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import baseFind from "../modules/core/selector.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Mask.js
var Mask = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("mask", node), attrs);
	}
	remove() {
		this.targets().forEach(function(el) {
			el.unmask();
		});
		return super.remove();
	}
	targets() {
		return baseFind("svg [mask*=" + this.id() + "]");
	}
};
registerMethods({
	Container: { mask: wrapWithAttrCheck(function() {
		return this.defs().put(new Mask());
	}) },
	Element: {
		masker() {
			return this.reference("mask");
		},
		maskWith(element) {
			const masker = element instanceof Mask ? element : this.parent().mask().add(element);
			return this.attr("mask", "url(#" + masker.id() + ")");
		},
		unmask() {
			return this.attr("mask", null);
		}
	}
});
register(Mask, "Mask");
//#endregion
export { Mask as default };
