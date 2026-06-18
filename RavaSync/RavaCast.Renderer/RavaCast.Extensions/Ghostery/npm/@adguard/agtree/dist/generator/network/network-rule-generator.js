import { BaseGenerator } from "../base-generator.js";
import { ModifierListGenerator } from "../misc/modifier-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/network/network-rule-generator.js
/**
* Generator for network rule nodes.
*/
var NetworkRuleGenerator = class extends BaseGenerator {
	/**
	* Generates a string from a network rule AST node.
	*
	* @param node Network rule node to generate a string from.
	* @returns Generated string representation of the network rule.
	*/
	static generate(node) {
		let result = "";
		if (node.exception) result += "@@";
		result += node.pattern.value;
		if (node.modifiers && node.modifiers.children.length > 0) {
			result += "$";
			result += ModifierListGenerator.generate(node.modifiers);
		}
		return result;
	}
};
//#endregion
export { NetworkRuleGenerator };
