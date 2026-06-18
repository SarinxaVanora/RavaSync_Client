import { AdblockSyntax } from "../../utils/adblockers.js";
import { BaseGenerator } from "../base-generator.js";
import { CosmeticRulePatternGenerator } from "./cosmetic-rule-pattern-generator.js";
import { CosmeticRuleBodyGenerator } from "./cosmetic-rule-body-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/cosmetic-rule-generator.js
/**
* `CosmeticRuleGenerator` is responsible for generating cosmetic rules from their AST representation.
*
* This class takes a parsed cosmetic rule Abstract Syntax Tree (AST) and converts it back into a raw string format.
* It handles the generation of the pattern, separator, uBO rule modifiers, and the rule body.
*/
var CosmeticRuleGenerator = class extends BaseGenerator {
	/**
	* Converts a cosmetic rule AST into a string.
	*
	* @param node Cosmetic rule AST
	* @returns Raw string
	*/
	static generate(node) {
		let result = "";
		result += CosmeticRulePatternGenerator.generate(node);
		result += node.separator.value;
		if (node.syntax === AdblockSyntax.Ubo && node.modifiers) {
			node.modifiers.children.forEach((modifier) => {
				if (modifier.exception) {
					result += ":";
					result += "not";
					result += "(";
				}
				result += ":";
				result += modifier.name.value;
				if (modifier.value) {
					result += "(";
					result += modifier.value.value;
					result += ")";
				}
				if (modifier.exception) result += ")";
			});
			if (node.modifiers.children.some((modifier) => modifier?.name.value)) result += " ";
		}
		result += CosmeticRuleBodyGenerator.generate(node);
		return result;
	}
};
//#endregion
export { CosmeticRuleGenerator };
