import { BaseGenerator } from "../../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/type-selector-generator.js
/**
* Type selector generator.
*/
var TypeSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the type selector.
	*
	* @param node Type selector node.
	*
	* @returns String representation of the type selector.
	*/
	static generate(node) {
		return node.value;
	}
};
//#endregion
export { TypeSelectorGenerator };
