import { BaseGenerator } from "../../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/id-selector-generator.js
/**
* ID selector generator.
*/
var IdSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the ID selector.
	*
	* @param node ID selector node.
	*
	* @returns String representation of the ID selector.
	*/
	static generate(node) {
		return "#" + node.value;
	}
};
//#endregion
export { IdSelectorGenerator };
