import { BaseGenerator } from "../../base-generator.js";
import { ComplexSelectorGenerator } from "./complex-selector-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/selector-list-generator.js
/**
* Selector list generator.
*/
var SelectorListGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the selector list.
	*
	* @param node Selector list node.
	*
	* @returns String representation of the selector list.
	*
	* @throws Error if the `node` is invalid.
	*/
	static generate(node) {
		if (node.children.length === 0) throw new Error("Selector list cannot be empty");
		const result = [];
		for (let i = 0; i < node.children.length; i += 1) {
			const complexSelector = node.children[i];
			if (i > 0) {
				result.push(",");
				result.push(" ");
			}
			result.push(ComplexSelectorGenerator.generate(complexSelector));
		}
		return result.join("");
	}
};
//#endregion
export { SelectorListGenerator };
