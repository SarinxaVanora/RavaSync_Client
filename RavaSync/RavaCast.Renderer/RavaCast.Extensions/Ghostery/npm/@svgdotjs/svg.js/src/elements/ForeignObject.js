import { registerMethods } from "../utils/methods.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import Element from "./Element.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/ForeignObject.js
var ForeignObject = class extends Element {
	constructor(node, attrs = node) {
		super(nodeOrNew("foreignObject", node), attrs);
	}
};
registerMethods({ Container: { foreignObject: wrapWithAttrCheck(function(width, height) {
	return this.put(new ForeignObject()).size(width, height);
}) } });
register(ForeignObject, "ForeignObject");
//#endregion
export { ForeignObject as default };
