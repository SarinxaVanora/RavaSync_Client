import { NetworkRuleType, RuleCategory } from "../nodes/index.js";
import { BaseGenerator } from "./base-generator.js";
import { CommentRuleGenerator } from "./comment/comment-rule-generator.js";
import { CosmeticRuleGenerator } from "./cosmetic/cosmetic-rule-generator.js";
import { HostRuleGenerator } from "./network/host-rule-generator.js";
import { NetworkRuleGenerator } from "./network/network-rule-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/rule-generator.js
/**
* RuleGenerator is responsible for converting adblock rule ASTs to their string representation.
*/
var RuleGenerator = class extends BaseGenerator {
	/**
	* Converts a rule AST to a string.
	*
	* @param ast - Adblock rule AST
	* @returns Raw string
	* @example
	* Take a look at the following example:
	* ```js
	* // Parse the rule to the AST
	* const ast = RuleParser.parse("example.org##.banner");
	* // Generate the rule from the AST
	* const raw = RuleParser.generate(ast);
	* // Print the generated rule
	* console.log(raw); // "example.org##.banner"
	* ```
	*/
	static generate(ast) {
		switch (ast.category) {
			case RuleCategory.Empty: return "";
			case RuleCategory.Invalid: return ast.raw;
			case RuleCategory.Comment: return CommentRuleGenerator.generate(ast);
			case RuleCategory.Cosmetic: return CosmeticRuleGenerator.generate(ast);
			case RuleCategory.Network: switch (ast.type) {
				case NetworkRuleType.HostRule: return HostRuleGenerator.generate(ast);
				case NetworkRuleType.NetworkRule: return NetworkRuleGenerator.generate(ast);
				default: throw new Error("Unknown network rule type");
			}
			default: throw new Error("Unknown rule category");
		}
	}
};
//#endregion
export { RuleGenerator };
