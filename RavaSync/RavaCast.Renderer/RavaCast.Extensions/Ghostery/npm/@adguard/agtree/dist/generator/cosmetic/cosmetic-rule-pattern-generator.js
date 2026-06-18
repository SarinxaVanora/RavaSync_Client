import { AdblockSyntax } from "../../utils/adblockers.js";
import { BaseGenerator } from "../base-generator.js";
import { ModifierListGenerator } from "../misc/modifier-list-generator.js";
import { DomainListGenerator } from "../misc/domain-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/cosmetic-rule-pattern-generator.js
/**
* Cosmetic rule pattern generator.
*/
var CosmeticRulePatternGenerator = class extends BaseGenerator {
	/**
	* Generates the rule pattern from the AST.
	*
	* @param node Cosmetic rule node
	* @returns Raw rule pattern
	* @example
	* - '##.foo' → ''
	* - 'example.com,example.org##.foo' → 'example.com,example.org'
	* - '[$path=/foo/bar]example.com##.foo' → '[$path=/foo/bar]example.com'
	*/
	static generate(node) {
		let result = "";
		if (node.syntax === AdblockSyntax.Adg && node.modifiers && node.modifiers.children.length > 0) {
			result += "[";
			result += "$";
			result += ModifierListGenerator.generate(node.modifiers);
			result += "]";
		}
		result += DomainListGenerator.generate(node.domains);
		return result;
	}
};
//#endregion
export { CosmeticRulePatternGenerator };
