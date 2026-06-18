import { NetworkRuleType, RuleCategory } from "../../nodes/index.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { NetworkRuleModifierListConverter } from "../misc/network-rule-modifier.js";
//#region node_modules/@adguard/agtree/dist/converter/network/index.js
/**
* @file Network rule converter
*/
/**
* Network rule converter class (also known as "basic rule converter")
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var NetworkRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a network rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		if (rule.type !== NetworkRuleType.NetworkRule) throw new Error(`Invalid rule type: ${rule.type}`);
		if (rule.modifiers) {
			const modifiers = NetworkRuleModifierListConverter.convertToAdg(rule.modifiers, rule.exception);
			if (modifiers.isConverted) return {
				result: [{
					category: RuleCategory.Network,
					type: NetworkRuleType.NetworkRule,
					syntax: rule.syntax,
					exception: rule.exception,
					pattern: {
						type: "Value",
						value: rule.pattern.value
					},
					modifiers: modifiers.result
				}],
				isConverted: true
			};
		}
		return createNodeConversionResult([rule], false);
	}
	/**
	* Converts a network rule to uBlock format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToUbo(rule) {
		if (rule.type !== NetworkRuleType.NetworkRule) throw new Error(`Invalid rule type: ${rule.type}`);
		if (rule.modifiers) {
			const modifiers = NetworkRuleModifierListConverter.convertToUbo(rule.modifiers, rule.exception);
			if (modifiers.isConverted) return {
				result: [{
					category: RuleCategory.Network,
					type: NetworkRuleType.NetworkRule,
					syntax: rule.syntax,
					exception: rule.exception,
					pattern: {
						type: "Value",
						value: rule.pattern.value
					},
					modifiers: modifiers.result
				}],
				isConverted: true
			};
		}
		return createNodeConversionResult([rule], false);
	}
};
//#endregion
export { NetworkRuleConverter };
