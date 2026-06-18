import "../../../../../../../virtual/_rolldown/runtime.js";
import { ADG_DOMAINS_MODIFIER, ADG_PATH_MODIFIER, UBO_MATCHES_PATH_OPERATOR } from "../../../utils/constants.js";
import { StringUtils } from "../../../utils/string.js";
import { ListItemNodeType, ListNodeType } from "../../../nodes/index.js";
import { require_sprintf } from "../../../../../../sprintf-js/src/sprintf.js";
import "../../../../../css-tokenizer/dist/csstokenizer.js";
import { DomainListParser } from "../../../parser/misc/domain-list-parser.js";
import "../../../../../../tldts/dist/es6/index.js";
import { require_is_ip } from "../../../../../../is-ip/index.js";
import { clone } from "../../../utils/clone.js";
import { createConversionResult } from "../../base-interfaces/conversion-result.js";
import { RegExpUtils } from "../../../utils/regexp.js";
import { createModifierNode } from "../../../ast-utils/modifiers.js";
import { MultiValueMap } from "../../../utils/multi-value-map.js";
require_sprintf();
require_is_ip();
/**
* @file Cosmetic rule modifier converter from ADG to uBO
*/
/**
* Regular expression pattern for matching the main page
* https://github.com/gorhill/uBlock/wiki/Procedural-cosmetic-filters#subjectmatches-patharg
*/
var UBO_MAIN_PAGE_MATCHER = "/^/$/";
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
* Helper class for converting cosmetic rule modifiers from ADG to uBO
*/
var UboCosmeticRuleModifierConverter = class {
	/**
	* Converts a ADG cosmetic rule modifier list to uBO, if possible.
	*
	* @param modifierList Cosmetic rule modifier list node to convert
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the converted node, and its `isConverted` flag indicates whether the original node was converted.
	* If the node was not converted, the result will contain the original node with the same object reference
	* @throws If the modifier list cannot be converted
	* @see {@link https://github.com/gorhill/uBlock/wiki/Procedural-cosmetic-filters#cosmetic-filter-operators}
	*/
	static convertFromAdg(modifierList) {
		const conversionMap = new MultiValueMap();
		let domainList = null;
		let regexDomainValue;
		modifierList.children.forEach((modifier, index) => {
			let value;
			let { exception } = modifier;
			switch (modifier.name.value) {
				case "app": throw new Error("The $app modifier is not supported by uBO");
				case ADG_DOMAINS_MODIFIER:
					if (!domainList) domainList = {
						type: ListNodeType.DomainList,
						separator: ",",
						children: [],
						start: modifier.start,
						end: modifier.end
					};
					if (!modifier?.value?.value) return;
					domainList = DomainListParser.parse(modifier.value.value, {}, modifier.start, "|");
					conversionMap.add(index, null);
					break;
				case "url":
					if (!domainList) domainList = {
						type: ListNodeType.DomainList,
						separator: ",",
						children: [],
						start: modifier.start,
						end: modifier.end
					};
					if (!modifier?.value?.value) return;
					regexDomainValue = RegExpUtils.patternToRegexp(modifier.value.value);
					domainList = {
						type: ListNodeType.DomainList,
						separator: ",",
						children: [{
							type: ListItemNodeType.Domain,
							value: RegExpUtils.ensureSlashes(regexDomainValue),
							exception: modifier?.exception ?? false
						}],
						start: modifier.start,
						end: modifier.end
					};
					conversionMap.add(index, null);
					break;
				case ADG_PATH_MODIFIER:
					if (!modifier.value) value = UBO_MAIN_PAGE_MATCHER;
					else if (RegExpUtils.isNegatedRegexPattern(modifier.value.value)) {
						exception = true;
						value = StringUtils.escapeCharacters(RegExpUtils.removeNegationFromRegexPattern(modifier.value.value), SPECIAL_MODIFIER_REGEX_CHARS);
					} else value = RegExpUtils.isRegexPattern(modifier.value.value) ? StringUtils.escapeCharacters(modifier.value.value, SPECIAL_MODIFIER_REGEX_CHARS) : modifier.value.value;
					conversionMap.add(index, createModifierNode(UBO_MATCHES_PATH_OPERATOR, value, exception));
					break;
			}
		});
		if (conversionMap.size) {
			const modifierListClone = clone(modifierList);
			modifierListClone.children = modifierListClone.children.map((modifier, index) => {
				return conversionMap.get(index) ?? modifier;
			}).flat().filter((modifier) => modifier !== null);
			return createConversionResult({
				modifierList: modifierListClone,
				domains: domainList || void 0
			}, true);
		}
		return createConversionResult({
			modifierList,
			domains: void 0
		}, false);
	}
};
//#endregion
export { UboCosmeticRuleModifierConverter };
