import { BaseGenerator } from "../../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/selector-combinator-generator.js
/**
* Selector combinator generator.
*/
var SelectorCombinatorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the selector combinator.
	*
	* @param node Selector combinator node.
	*
	* @returns String representation of the selector combinator.
	*/
	static generate(node) {
		if (node.value === " ") return node.value;
		return " " + node.value + " ";
	}
};
//#endregion
export { SelectorCombinatorGenerator };
