import { nodeOrNew, register } from "../utils/adopter.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Defs.js
var Defs = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("defs", node), attrs);
	}
	flatten() {
		return this;
	}
	ungroup() {
		return this;
	}
};
register(Defs, "Defs");
//#endregion
export { Defs as default };
