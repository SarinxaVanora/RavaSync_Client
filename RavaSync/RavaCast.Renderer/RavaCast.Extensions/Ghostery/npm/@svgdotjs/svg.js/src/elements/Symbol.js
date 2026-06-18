import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Container from "./Container.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Symbol.js
var Symbol = class extends Container {
	constructor(node, attrs = node) {
		super(nodeOrNew("symbol", node), attrs);
	}
};
registerMethods({ Container: { symbol: wrapWithAttrCheck(function() {
	return this.put(new Symbol());
}) } });
register(Symbol, "Symbol");
//#endregion
export { Symbol as default };
