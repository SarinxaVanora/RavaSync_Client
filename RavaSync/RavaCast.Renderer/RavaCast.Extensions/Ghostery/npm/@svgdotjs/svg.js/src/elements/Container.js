import { register } from "../utils/adopter.js";
import Element from "./Element.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Container.js
var Container = class Container extends Element {
	flatten() {
		this.each(function() {
			if (this instanceof Container) return this.flatten().ungroup();
		});
		return this;
	}
	ungroup(parent = this.parent(), index = parent.index(this)) {
		index = index === -1 ? parent.children().length : index;
		this.each(function(i, children) {
			return children[children.length - i - 1].toParent(parent, index);
		});
		return this.remove();
	}
};
register(Container, "Container");
//#endregion
export { Container as default };
