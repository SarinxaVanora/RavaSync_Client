import { CommentMarker, CommentRuleType } from "../../nodes/index.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { clone } from "../../utils/clone.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
//#region node_modules/@adguard/agtree/dist/converter/comment/index.js
/**
* @file Comment rule converter
*/
/**
* Comment rule converter class
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var CommentRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a comment rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		switch (rule.type) {
			case CommentRuleType.CommentRule:
				if (rule.type === CommentRuleType.CommentRule && rule.marker.value === CommentMarker.Hashmark) {
					const ruleClone = clone(rule);
					ruleClone.marker.value = CommentMarker.Regular;
					ruleClone.text.value = ` ${CommentMarker.Hashmark}${ruleClone.text.value}`;
					return createNodeConversionResult([ruleClone], true);
				}
				return createNodeConversionResult([rule], false);
			default: return createNodeConversionResult([rule], false);
		}
	}
};
//#endregion
export { CommentRuleConverter };
