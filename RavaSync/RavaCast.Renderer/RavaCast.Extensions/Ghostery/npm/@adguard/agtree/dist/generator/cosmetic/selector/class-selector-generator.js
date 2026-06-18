import { BaseGenerator } from "../../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/class-selector-generator.js
/**
* Class selector generator.
*/
var ClassSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the class selector.
	*
	* @param node Class selector node.
	*
	* @returns String representation of the class selector.
	*/
	static generate(node) {
		return "." + node.value;
	}
};
//#endregion
export { ClassSelectorGenerator };
