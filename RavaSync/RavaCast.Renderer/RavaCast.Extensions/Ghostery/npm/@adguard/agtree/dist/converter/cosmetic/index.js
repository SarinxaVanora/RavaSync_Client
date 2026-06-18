import "../../../../../../virtual/_rolldown/runtime.js";
import { AdblockSyntax } from "../../utils/adblockers.js";
import { CosmeticRuleSeparator, CosmeticRuleType, RuleCategory } from "../../nodes/index.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import "../../../../css-tokenizer/dist/csstokenizer.js";
import "../../../../../tldts/dist/es6/index.js";
import { require_glob_to_regexp } from "../../../../../glob-to-regexp/index.js";
import { RuleConversionError } from "../../errors/rule-conversion-error.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { clone } from "../../utils/clone.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { HtmlRuleConverter } from "./html.js";
import { ScriptletRuleConverter } from "./scriptlet.js";
import { AdgCosmeticRuleModifierConverter } from "./rule-modifiers/adg.js";
import { CssInjectionRuleConverter } from "./css.js";
import { ElementHidingRuleConverter } from "./element-hiding.js";
import { HeaderRemovalRuleConverter } from "./header-removal.js";
import { UboCosmeticRuleModifierConverter } from "./rule-modifiers/ubo.js";
require_glob_to_regexp();
require_sprintf();
/**
* @file Cosmetic rule converter
*/
/**
* Cosmetic rule converter class (also known as "non-basic rule converter")
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var CosmeticRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a cosmetic rule to AdGuard syntax, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		let subconverterResult;
		switch (rule.type) {
			case CosmeticRuleType.ElementHidingRule:
				subconverterResult = ElementHidingRuleConverter.convertToAdg(rule);
				break;
			case CosmeticRuleType.ScriptletInjectionRule:
				subconverterResult = ScriptletRuleConverter.convertToAdg(rule);
				break;
			case CosmeticRuleType.CssInjectionRule:
				subconverterResult = CssInjectionRuleConverter.convertToAdg(rule);
				break;
			case CosmeticRuleType.HtmlFilteringRule:
				subconverterResult = HeaderRemovalRuleConverter.convertToAdg(rule);
				if (subconverterResult.isConverted) break;
				subconverterResult = HtmlRuleConverter.convertToAdg(rule);
				break;
			case CosmeticRuleType.JsInjectionRule:
				subconverterResult = createNodeConversionResult([rule], false);
				break;
			default: throw new RuleConversionError("Unsupported cosmetic rule type");
		}
		let convertedModifiers;
		if (rule.modifiers) {
			if (rule.syntax === AdblockSyntax.Ubo) {
				if (rule.type === CosmeticRuleType.ScriptletInjectionRule) throw new RuleConversionError("uBO scriptlet injection rules don't support cosmetic rule modifiers");
				convertedModifiers = AdgCosmeticRuleModifierConverter.convertFromUbo(rule.modifiers);
			} else if (rule.syntax === AdblockSyntax.Abp) throw new RuleConversionError("ABP don't support cosmetic rule modifiers");
		}
		if (subconverterResult.result.length > 1 || subconverterResult.isConverted || convertedModifiers && convertedModifiers.isConverted) {
			subconverterResult.result.forEach((subconverterRule) => {
				if (convertedModifiers && subconverterRule.category === RuleCategory.Cosmetic) subconverterRule.modifiers = convertedModifiers.result;
			});
			return subconverterResult;
		}
		return createNodeConversionResult([rule], false);
	}
	/**
	* Converts a cosmetic rule to uBlock Origin syntax, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToUbo(rule) {
		if (rule.syntax === AdblockSyntax.Ubo) return createNodeConversionResult([rule], false);
		switch (rule.type) {
			case CosmeticRuleType.HtmlFilteringRule: return HtmlRuleConverter.convertToUbo(rule);
			case CosmeticRuleType.ElementHidingRule:
				if ((rule.separator.value === CosmeticRuleSeparator.ElementHidingException || rule.separator.value === CosmeticRuleSeparator.ElementHiding) && !rule.modifiers) return createNodeConversionResult([rule], false);
				break;
			case CosmeticRuleType.ScriptletInjectionRule: return ScriptletRuleConverter.convertToUbo(rule);
			case CosmeticRuleType.JsInjectionRule: throw new RuleConversionError("uBO does not support JS injection rules");
		}
		let convertedModifiers;
		if (rule.modifiers) {
			if (rule.syntax === AdblockSyntax.Abp) throw new RuleConversionError("ABP does not support cosmetic rule modifiers");
			else if (rule.syntax === AdblockSyntax.Adg) convertedModifiers = UboCosmeticRuleModifierConverter.convertFromAdg(rule.modifiers);
		}
		const result = clone(rule);
		result.syntax = AdblockSyntax.Ubo;
		if (convertedModifiers && convertedModifiers.isConverted) {
			result.modifiers = convertedModifiers.result.modifierList;
			if (convertedModifiers.result.domains) {
				result.domains = convertedModifiers.result.domains;
				result.domains.separator = ",";
			}
		}
		let convertedSeparator = result.separator.value;
		convertedSeparator = rule.exception ? CosmeticRuleSeparator.ElementHidingException : CosmeticRuleSeparator.ElementHiding;
		result.separator.value = convertedSeparator;
		return createNodeConversionResult([result], true);
	}
};
//#endregion
export { CosmeticRuleConverter };
