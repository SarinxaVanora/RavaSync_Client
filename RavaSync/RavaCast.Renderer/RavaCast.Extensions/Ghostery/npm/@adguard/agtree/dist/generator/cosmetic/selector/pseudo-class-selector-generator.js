import { BaseGenerator } from "../../base-generator.js";
import { ValueGenerator } from "../../misc/value-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/pseudo-class-selector-generator.js
/**
* Pseudo-class selector generator.
*/
var PseudoClassSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the pseudo-class selector.
	*
	* @param node Pseudo-class selector node.
	*
	* @returns String representation of the pseudo-class selector.
	*/
	static generate(node) {
		const result = [];
		result.push(":");
		result.push(ValueGenerator.generate(node.name));
		if (node.argument) {
			result.push("(");
			result.push(ValueGenerator.generate(node.argument));
			result.push(")");
		}
		return result.join("");
	}
};
//#endregion
export { PseudoClassSelectorGenerator };
