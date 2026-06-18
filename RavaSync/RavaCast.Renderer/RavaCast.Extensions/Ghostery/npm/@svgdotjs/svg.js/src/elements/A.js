import { registerMethods } from "../utils/methods.js";
import { xlink } from "../modules/core/namespaces.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Container from "./Container.js";
import { containerGeometry_exports } from "../modules/core/containerGeometry.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/A.js
var A = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("a", node), attrs);
	}
	target(target) {
		return this.attr("target", target);
	}
	to(url) {
		return this.attr("href", url, xlink);
	}
};
extend(A, containerGeometry_exports);
registerMethods({
	Container: { link: wrapWithAttrCheck(function(url) {
		return this.put(new A()).to(url);
	}) },
	Element: {
		unlink() {
			const link = this.linker();
			if (!link) return this;
			const parent = link.parent();
			if (!parent) return this.remove();
			const index = parent.index(link);
			parent.add(this, index);
			link.remove();
			return this;
		},
		linkTo(url) {
			let link = this.linker();
			if (!link) {
				link = new A();
				this.wrap(link);
			}
			if (typeof url === "function") url.call(link, link);
			else link.to(url);
			return this;
		},
		linker() {
			const link = this.parent();
			if (link && link.node.nodeName.toLowerCase() === "a") return link;
			return null;
		}
	}
});
register(A, "A");
//#endregion
export { A as default };
