import { StringUtils } from "../../../utils/string.js";
import { RuleConversionError } from "../../../errors/rule-conversion-error.js";
import { clone } from "../../../utils/clone.js";
import { createConversionResult } from "../../base-interfaces/conversion-result.js";
import { RegExpUtils } from "../../../utils/regexp.js";
import { createModifierNode } from "../../../ast-utils/modifiers.js";
import { MultiValueMap } from "../../../utils/multi-value-map.js";
//#region node_modules/@adguard/agtree/dist/converter/cosmetic/rule-modifiers/adg.js
/**
* @file Cosmetic rule modifier converter from uBO to ADG
*/
var UBO_MATCHES_PATH_OPERATOR = "matches-path";
var ADG_PATH_MODIFIER = "path";
/**
* Special characters in modifier regexps that should be escaped
*/
var SPECIAL_MODIFIER_REGEX_CHARS = new Set([
	"[",
	"]",
	",",
	"\\"
]);
/**
* Helper class for converting cosmetic rule modifiers from uBO to ADG
*/
var AdgCosmeticRuleModifierConverter = class {
	/**
	* Converts a uBO cosmetic rule modifier list to ADG, if possible.
	*
	* @param modifierList Cosmetic rule modifier list node to convert
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the converted node, and its `isConverted` flag indicates whether the original node was converted.
	* If the node was not converted, the result will contain the original node with the same object reference
	* @throws If the modifier list cannot be converted
	* @see {@link https://github.com/gorhill/uBlock/wiki/Procedural-cosmetic-filters#cosmetic-filter-operators}
	*/
	static convertFromUbo(modifierList) {
		const conversionMap = new MultiValueMap();
		modifierList.children.forEach((modifier, index) => {
			if (modifier.name.value === UBO_MATCHES_PATH_OPERATOR) {
				if (!modifier.value) throw new RuleConversionError(`'${UBO_MATCHES_PATH_OPERATOR}' operator requires a value`);
				const value = RegExpUtils.isRegexPattern(modifier.value.value) ? StringUtils.escapeCharacters(modifier.value.value, SPECIAL_MODIFIER_REGEX_CHARS) : modifier.value.value;
				conversionMap.add(index, createModifierNode(ADG_PATH_MODIFIER, modifier.exception ? `/${RegExpUtils.negateRegexPattern(RegExpUtils.patternToRegexp(value))}/` : value));
			}
		});
		if (conversionMap.size) {
			const modifierListClone = clone(modifierList);
			modifierListClone.children = modifierListClone.children.map((modifier, index) => {
				return conversionMap.get(index) ?? modifier;
			}).flat();
			return createConversionResult(modifierListClone, true);
		}
		return createConversionResult(modifierList, false);
	}
};
//#endregion
export { AdgCosmeticRuleModifierConverter };
