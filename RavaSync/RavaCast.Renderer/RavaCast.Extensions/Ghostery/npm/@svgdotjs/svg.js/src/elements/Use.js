import { registerMethods } from "../utils/methods.js";
import { xlink } from "../modules/core/namespaces.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Shape from "./Shape.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Use.js
var Use = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("use", node), attrs);
	}
	use(element, file) {
		return this.attr("href", (file || "") + "#" + element, xlink);
	}
};
registerMethods({ Container: { use: wrapWithAttrCheck(function(element, file) {
	return this.put(new Use()).use(element, file);
}) } });
register(Use, "Use");
//#endregion
export { Use as default };
