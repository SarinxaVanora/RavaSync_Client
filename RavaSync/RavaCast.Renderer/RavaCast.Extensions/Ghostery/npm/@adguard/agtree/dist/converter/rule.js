import { NetworkRuleType, RuleCategory } from "../nodes/index.js";
import { RuleConversionError } from "../errors/rule-conversion-error.js";
import { RuleConverterBase } from "./base-interfaces/rule-converter-base.js";
import { createConversionResult } from "./base-interfaces/conversion-result.js";
import { CommentRuleConverter } from "./comment/index.js";
import { CosmeticRuleConverter } from "./cosmetic/index.js";
import { NetworkRuleConverter } from "./network/index.js";
//#region node_modules/@adguard/agtree/dist/converter/rule.js
/**
* @file Adblock rule converter
*
* This file is the entry point for all rule converters
* which automatically detects the rule type and calls
* the corresponding "sub-converter".
*/
/**
* Adblock filtering rule converter class
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var RuleConverter = class extends RuleConverterBase {
	/**
	* Converts an adblock filtering rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		switch (rule.category) {
			case RuleCategory.Comment: return CommentRuleConverter.convertToAdg(rule);
			case RuleCategory.Cosmetic: return CosmeticRuleConverter.convertToAdg(rule);
			case RuleCategory.Network:
				if (rule.type === NetworkRuleType.HostRule) return createConversionResult([rule], false);
				return NetworkRuleConverter.convertToAdg(rule);
			case RuleCategory.Invalid:
			case RuleCategory.Empty: return createConversionResult([rule], false);
			default: throw new RuleConversionError("Unknown rule category");
		}
	}
	/**
	* Converts an adblock filtering rule to uBlock Origin format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToUbo(rule) {
		if (rule.category === RuleCategory.Cosmetic) return CosmeticRuleConverter.convertToUbo(rule);
		if (rule.category === RuleCategory.Network) return NetworkRuleConverter.convertToUbo(rule);
		return createConversionResult([rule], false);
	}
};
//#endregion
export { RuleConverter };
