import { AdblockSyntax } from "../../utils/adblockers.js";
import { CosmeticRuleSeparator } from "../../nodes/index.js";
import { CssTokenStream } from "../../parser/css/css-token-stream.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { clone } from "../../utils/clone.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { CssSelectorConverter } from "../css/index.js";
//#region node_modules/@adguard/agtree/dist/converter/cosmetic/css.js
/**
* @file CSS injection rule converter
*/
/**
* CSS injection rule converter class
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var CssInjectionRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a CSS injection rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		const separator = rule.separator.value;
		let convertedSeparator = separator;
		const stream = new CssTokenStream(rule.body.selectorList.value);
		const convertedSelectorList = CssSelectorConverter.convertToAdg(stream);
		if (stream.hasAnySelectorExtendedCssNodeStrict() || rule.body.remove) convertedSeparator = rule.exception ? CosmeticRuleSeparator.AdgExtendedCssInjectionException : CosmeticRuleSeparator.AdgExtendedCssInjection;
		else if (rule.syntax !== AdblockSyntax.Adg) convertedSeparator = rule.exception ? CosmeticRuleSeparator.AdgCssInjectionException : CosmeticRuleSeparator.AdgCssInjection;
		if (!(rule.syntax === AdblockSyntax.Common || rule.syntax === AdblockSyntax.Adg) || separator !== convertedSeparator || convertedSelectorList.isConverted) {
			const ruleClone = clone(rule);
			ruleClone.syntax = AdblockSyntax.Adg;
			ruleClone.separator.value = convertedSeparator;
			ruleClone.body.selectorList.value = convertedSelectorList.result;
			return createNodeConversionResult([ruleClone], true);
		}
		return createNodeConversionResult([rule], false);
	}
};
//#endregion
export { CssInjectionRuleConverter };
