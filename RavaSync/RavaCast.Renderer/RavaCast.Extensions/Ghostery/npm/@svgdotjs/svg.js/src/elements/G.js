import { registerMethods } from "../utils/methods.js";
import { extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Container from "./Container.js";
import { containerGeometry_exports } from "../modules/core/containerGeometry.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/G.js
var G = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("g", node), attrs);
	}
};
extend(G, containerGeometry_exports);
registerMethods({ Container: { group: wrapWithAttrCheck(function() {
	return this.put(new G());
}) } });
register(G, "G");
//#endregion
export { G as default };
