import { AdblockSyntax } from "../../utils/adblockers.js";
import { CosmeticRuleType } from "../../nodes/index.js";
import { UboPseudoName } from "../../common/ubo-selector-common.js";
import { BaseGenerator } from "../base-generator.js";
import { AdgScriptletInjectionBodyGenerator } from "./scriptlet-body/adg-scriptlet-injection-body-generator.js";
import { AdgCssInjectionGenerator } from "../css/adg-css-injection-generator.js";
import { AbpSnippetInjectionBodyGenerator } from "./scriptlet-body/abp-snippet-injection-body-generator.js";
import { UboScriptletInjectionBodyGenerator } from "./scriptlet-body/ubo-scriptlet-injection-body-generator.js";
import { AdgHtmlFilteringBodyGenerator } from "./html-filtering-body/adg-html-filtering-body-generator.js";
import { UboHtmlFilteringBodyGenerator } from "./html-filtering-body/ubo-html-filtering-body-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/cosmetic-rule-body-generator.js
/**
* Cosmetic rule body generator.
*/
var CosmeticRuleBodyGenerator = class extends BaseGenerator {
	/**
	* Generates the rule body from the node.
	*
	* @param node Cosmetic rule node
	* @returns Raw rule body
	* @example
	* - '##.foo' → '.foo'
	* - 'example.com,example.org##.foo' → '.foo'
	* - 'example.com#%#//scriptlet('foo')' → '//scriptlet('foo')'
	*
	* @throws Error if the rule type is unknown
	*/
	static generate(node) {
		let result = "";
		switch (node.type) {
			case CosmeticRuleType.ElementHidingRule:
				result = node.body.selectorList.value;
				break;
			case CosmeticRuleType.CssInjectionRule:
				if (node.syntax === AdblockSyntax.Adg || node.syntax === AdblockSyntax.Abp) result = AdgCssInjectionGenerator.generate(node.body);
				else if (node.syntax === AdblockSyntax.Ubo) {
					if (node.body.mediaQueryList) {
						result += ":";
						result += UboPseudoName.MatchesMedia;
						result += "(";
						result += node.body.mediaQueryList.value;
						result += ")";
						result += " ";
					}
					result += node.body.selectorList.value;
					if (node.body.remove) {
						result += ":";
						result += UboPseudoName.Remove;
						result += "(";
						result += ")";
					} else if (node.body.declarationList) {
						result += ":";
						result += UboPseudoName.Style;
						result += "(";
						result += node.body.declarationList.value;
						result += ")";
					}
				}
				break;
			case CosmeticRuleType.HtmlFilteringRule:
				switch (node.syntax) {
					case AdblockSyntax.Adg:
						result = AdgHtmlFilteringBodyGenerator.generate(node.body);
						break;
					case AdblockSyntax.Ubo:
						result = "^" + UboHtmlFilteringBodyGenerator.generate(node.body);
						break;
					case AdblockSyntax.Abp: throw new Error("ABP does not support HTML filtering rules");
					default: throw new Error("HTML filtering rule should have an explicit syntax");
				}
				break;
			case CosmeticRuleType.JsInjectionRule:
				result = node.body.value;
				break;
			case CosmeticRuleType.ScriptletInjectionRule:
				switch (node.syntax) {
					case AdblockSyntax.Adg:
						result = AdgScriptletInjectionBodyGenerator.generate(node.body);
						break;
					case AdblockSyntax.Abp:
						result = AbpSnippetInjectionBodyGenerator.generate(node.body);
						break;
					case AdblockSyntax.Ubo:
						result = UboScriptletInjectionBodyGenerator.generate(node.body);
						break;
					default: throw new Error("Scriptlet rule should have an explicit syntax");
				}
				break;
			default: throw new Error("Unknown cosmetic rule type");
		}
		return result;
	}
};
//#endregion
export { CosmeticRuleBodyGenerator };
