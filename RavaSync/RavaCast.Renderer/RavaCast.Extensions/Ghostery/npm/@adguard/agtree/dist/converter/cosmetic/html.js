import { AdblockSyntax } from "../../utils/adblockers.js";
import { CosmeticRuleSeparator, CosmeticRuleType, RuleCategory } from "../../nodes/index.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { AdgHtmlFilteringBodyParser } from "../../parser/cosmetic/html-filtering-body/adg-html-filtering-body-parser.js";
import { UboHtmlFilteringBodyParser } from "../../parser/cosmetic/html-filtering-body/ubo-html-filtering-body-parser.js";
import { AdgHtmlFilteringBodyGenerator } from "../../generator/cosmetic/html-filtering-body/adg-html-filtering-body-generator.js";
import { UboHtmlFilteringBodyGenerator } from "../../generator/cosmetic/html-filtering-body/ubo-html-filtering-body-generator.js";
import { RuleConversionError } from "../../errors/rule-conversion-error.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { cloneDomainListNode } from "../../ast-utils/clone.js";
import { RegExpUtils } from "../../utils/regexp.js";
//#region node_modules/@adguard/agtree/dist/converter/cosmetic/html.js
var import_sprintf = require_sprintf();
var ADG_HTML_CONVERSION_MAX_LENGTH = 8192 * 32;
/**
* Supported special pseudo-classes from uBlock.
*
* Note: If new pseudo-classes are added here, ensure to update
* the set and logic in the converter methods accordingly.
*/
var UboPseudoClasses = {
	HasText: "has-text",
	MinTextLength: "min-text-length"
};
/**
* Supported special attribute selectors from AdGuard.
*
* Note: If new pseudo-classes are added here, ensure to update
* the set and logic in the converter methods accordingly.
*/
var AdgAttributeSelectors = {
	MaxLength: "max-length",
	MinLength: "min-length",
	TagContent: "tag-content",
	Wildcard: "wildcard"
};
/**
* Supported special pseudo-classes from AdGuard.
*
* Note: If new pseudo-classes are added here, ensure to update
* the set and logic in the converter methods accordingly.
*/
var AdgPseudoClasses = { Contains: "contains" };
/**
* Set of {@link UboPseudoClasses}.
*/
var SUPPORTED_UBO_PSEUDO_CLASSES = new Set([UboPseudoClasses.HasText, UboPseudoClasses.MinTextLength]);
/**
* Set of {@link AdgAttributeSelectors}.
*/
var SUPPORTED_ADG_ATTRIBUTE_SELECTORS = new Set([
	AdgAttributeSelectors.MaxLength,
	AdgAttributeSelectors.MinLength,
	AdgAttributeSelectors.TagContent,
	AdgAttributeSelectors.Wildcard
]);
/**
* Set of {@link AdgPseudoClasses}.
*/
var SUPPORTED_ADG_PSEUDO_CLASSES = new Set([AdgPseudoClasses.Contains]);
/**
* Error messages used in HTML filtering rule conversion.
*/
var ERROR_MESSAGES = {
	ABP_NOT_SUPPORTED: "Invalid rule, ABP does not support HTML filtering rules",
	INVALID_RULE: "Invalid HTML filtering rule: %s",
	MIXED_SYNTAX_ADG_UBO: "Mixed AdGuard and uBlock syntax",
	EMPTY_SELECTOR_LIST: "Selector list of HTML filtering rule must not be empty",
	EMPTY_COMPLEX_SELECTOR: "Complex selector of selector list must not be empty",
	INVALID_SELECTOR_COMBINATOR: "Invalid selector combinator '%s' used between selectors",
	UNKNOWN_SELECTOR_TYPE: "Unknown selector type '%s' found during conversion",
	SPECIAL_ATTRIBUTE_SELECTOR_OPERATOR_INVALID: "Special attribute selector '%s' has invalid operator '%s'",
	SPECIAL_ATTRIBUTE_SELECTOR_FLAG_NOT_SUPPORTED: "Special attribute selector '%s' does not support flags",
	SPECIAL_ATTRIBUTE_SELECTOR_VALUE_REQUIRED: "Special attribute selector '%s' requires a value",
	SPECIAL_ATTRIBUTE_SELECTOR_VALUE_INT: "Value of special attribute selector '%s' must be an integer, got '%s'",
	SPECIAL_ATTRIBUTE_SELECTOR_VALUE_POSITIVE: "Value of special attribute selector '%s' must be a positive integer, got '%s'",
	SPECIAL_ATTRIBUTE_SELECTOR_NOT_SUPPORTED: "Special attribute selector '%s' is not supported in conversion",
	SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_REQUIRED: "Special pseudo-class selector '%s' requires an argument",
	SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_INT: "Argument of special pseudo-class selector '%s' must be an integer, got '%s'",
	SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_POSITIVE: "Argument of special pseudo-class selector '%s' must be a positive integer, got '%s'",
	SPECIAL_PSEUDO_CLASS_SELECTOR_NOT_SUPPORTED: "Special pseudo-class selector '%s' is not supported in conversion"
};
/**
* HTML filtering rule converter class
*
* @todo Implement `convertToUbo` (ABP currently doesn't support HTML filtering rules)
*/
var HtmlRuleConverter = class HtmlRuleConverter extends RuleConverterBase {
	/**
	* Converts a HTML rule to AdGuard syntax, if possible.
	* Also can be used to convert AdGuard rules to AdGuard syntax to validate them.
	*
	* @param rule Rule node to convert.
	*
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference.
	*
	* @throws If the rule is invalid or cannot be converted.
	*/
	static convertToAdg(rule) {
		let parser;
		let onSpecialAttributeSelector;
		let onSpecialPseudoClassSelector;
		let isConverted = false;
		if (rule.syntax === AdblockSyntax.Adg) {
			parser = AdgHtmlFilteringBodyParser;
			onSpecialAttributeSelector = (name, value) => {
				/**
				* Mark rule as converted in ADG -> ADG conversion only if
				* special attribute selectors are present in the rule body,
				* because they are deprecated and will be removed soon,
				* so we convert them to pseudo-class selectors
				*/
				isConverted = true;
				return HtmlRuleConverter.convertSpecialAttributeSelectorAdgToAdg(name, value);
			};
			onSpecialPseudoClassSelector = HtmlRuleConverter.convertSpecialPseudoClassSelectorAdgToAdg;
		} else if (rule.syntax === AdblockSyntax.Ubo) {
			/**
			* Always mark rule as converted in UBO -> ADG conversion.
			*/
			isConverted = true;
			parser = UboHtmlFilteringBodyParser;
			onSpecialAttributeSelector = HtmlRuleConverter.convertSpecialAttributeSelectorUboToAdg;
			onSpecialPseudoClassSelector = HtmlRuleConverter.convertSpecialPseudoClassSelectorUboToAdg;
		} else throw new RuleConversionError(ERROR_MESSAGES.ABP_NOT_SUPPORTED);
		const convertedBody = HtmlRuleConverter.convertBody(rule.body, parser, AdgHtmlFilteringBodyGenerator, onSpecialAttributeSelector, onSpecialPseudoClassSelector);
		if (!isConverted) return createNodeConversionResult([rule], false);
		return createNodeConversionResult([{
			category: RuleCategory.Cosmetic,
			type: CosmeticRuleType.HtmlFilteringRule,
			syntax: AdblockSyntax.Adg,
			exception: rule.exception,
			domains: cloneDomainListNode(rule.domains),
			separator: {
				type: "Value",
				value: rule.exception ? CosmeticRuleSeparator.AdgHtmlFilteringException : CosmeticRuleSeparator.AdgHtmlFiltering
			},
			body: convertedBody
		}], true);
	}
	/**
	* Converts a HTML rule to uBlock syntax, if possible.
	* Also can be used to convert uBlock rules to uBlock syntax to validate them.
	*
	* @param rule Rule node to convert.
	*
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference.
	*
	* @throws Error if the rule is invalid or cannot be converted.
	*/
	static convertToUbo(rule) {
		if (rule.syntax === AdblockSyntax.Ubo) return createNodeConversionResult([rule], false);
		if (rule.syntax === AdblockSyntax.Abp) throw new RuleConversionError(ERROR_MESSAGES.ABP_NOT_SUPPORTED);
		const convertedBody = HtmlRuleConverter.convertBody(rule.body, AdgHtmlFilteringBodyParser, UboHtmlFilteringBodyGenerator, HtmlRuleConverter.convertSpecialAttributeSelectorAdgToUbo, HtmlRuleConverter.convertSpecialPseudoClassSelectorAdgToUbo);
		return createNodeConversionResult([{
			category: RuleCategory.Cosmetic,
			type: CosmeticRuleType.HtmlFilteringRule,
			syntax: AdblockSyntax.Ubo,
			exception: rule.exception,
			domains: cloneDomainListNode(rule.domains),
			separator: {
				type: "Value",
				value: rule.exception ? CosmeticRuleSeparator.ElementHidingException : CosmeticRuleSeparator.ElementHiding
			},
			body: convertedBody
		}], true);
	}
	/**
	* Handles special attribute selectors during AdGuard to AdGuard conversion:
	* - `[tag-content="content"]` -> `:contains(content)`
	*   direct conversion, no changes to value
	* - `[wildcard="*content*"]` -> `:contains(/*.content*./s)`
	*   convert search pattern to regular expression
	* - `[min-length="min"]` -> `:contains(/^(?=.{min,}$).*\/s)`
	*   converts to a length-matching regular expression
	* - `[max-length="max"]` -> `:contains(/^(?=.{0,max}$).*\/s)`
	*   converts to a length-matching regular expression
	*
	* Note: This attribute selector to pseudo-class selector conversion
	* is needed because AdGuard special attribute selectors are going
	* to be deprecated and removed soon.
	*
	* @param name Name of the special attribute selector.
	* @param value Value of the special attribute selector.
	*
	* @returns A {@link SimpleSelector} to add to the current complex selector.
	*/
	static convertSpecialAttributeSelectorAdgToAdg(name, value) {
		switch (name) {
			case AdgAttributeSelectors.TagContent: return HtmlRuleConverter.getPseudoClassSelectorNode(AdgPseudoClasses.Contains, value);
			case AdgAttributeSelectors.Wildcard: return HtmlRuleConverter.getPseudoClassSelectorNode(AdgPseudoClasses.Contains, RegExpUtils.globToRegExp(value));
			case AdgAttributeSelectors.MinLength:
			case AdgAttributeSelectors.MaxLength: {
				HtmlRuleConverter.assertValidLengthValue(name, value, ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_VALUE_INT, ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_VALUE_POSITIVE);
				const length = Number(value);
				let min = null;
				let max = null;
				if (name === AdgAttributeSelectors.MinLength) min = length;
				else max = length;
				return HtmlRuleConverter.getPseudoClassSelectorNode(AdgPseudoClasses.Contains, RegExpUtils.getLengthRegexp(min, max));
			}
			default: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_NOT_SUPPORTED, name));
		}
	}
	/**
	* Since special pseudo-class selectors do not need conversion
	* in AdGuard to AdGuard conversion, we simply return `true` to keep them as-is.
	*
	* @param name Name of the special pseudo-class selector.
	*
	* @returns `true` to keep the special pseudo-class selector as-is.
	*
	* @throws Rule conversion error for mixed syntax.
	*/
	static convertSpecialPseudoClassSelectorAdgToAdg(name) {
		if (SUPPORTED_UBO_PSEUDO_CLASSES.has(name)) throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, ERROR_MESSAGES.MIXED_SYNTAX_ADG_UBO));
		return true;
	}
	/**
	* Since special attribute selectors only AdGuard-specific,
	* we should never encounter them in uBlock rules.
	*
	* @throws Rule conversion error for mixed syntax.
	*/
	static convertSpecialAttributeSelectorUboToAdg() {
		throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, ERROR_MESSAGES.MIXED_SYNTAX_ADG_UBO));
	}
	/**
	* Handles special pseudo-class selectors during uBlock to AdGuard conversion:
	* - `:has-text(text)` -> `:contains(text)`
	*   direct conversion, no changes to argument
	* - `:min-text-length(min)` -> `:contains(/^(?=.{min,MAX_CONVERSION_DEFAULT}$).*\/s)`
	*   converts to a length-matching regular expression
	*
	* @param name Name of the special pseudo-class selector.
	* @param argument Argument of the special pseudo-class selector.
	*
	* @returns A {@link SimpleSelector} to add to the current complex selector.
	*
	* @throws If AdGuard-specific pseudo-class selector is found in uBlock rule.
	*/
	static convertSpecialPseudoClassSelectorUboToAdg(name, argument) {
		switch (name) {
			case UboPseudoClasses.HasText: return HtmlRuleConverter.getPseudoClassSelectorNode(AdgPseudoClasses.Contains, argument);
			case UboPseudoClasses.MinTextLength: {
				HtmlRuleConverter.assertValidLengthValue(name, argument, ERROR_MESSAGES.SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_INT, ERROR_MESSAGES.SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_POSITIVE);
				const minLength = Number(argument);
				return HtmlRuleConverter.getPseudoClassSelectorNode(AdgPseudoClasses.Contains, RegExpUtils.getLengthRegexp(minLength, ADG_HTML_CONVERSION_MAX_LENGTH));
			}
			case AdgPseudoClasses.Contains: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, ERROR_MESSAGES.MIXED_SYNTAX_ADG_UBO));
			default: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_PSEUDO_CLASS_SELECTOR_NOT_SUPPORTED, name));
		}
	}
	/**
	* Handles special attribute selectors during AdGuard to uBlock conversion:
	* - `[tag-content="content"]` -> `:has-text(content)`
	*   direct conversion, no changes to value
	* - `[wildcard="*content*"]` -> `:has-text(/*.content*./s)`
	*   convert search pattern to regular expression
	* - `[min-length="min"]` -> `:min-text-length(min)`
	*   direct conversion, no changes to value
	* - `[max-length]` is skipped
	*
	* @param name Name of the special attribute selector.
	* @param value Value of the special attribute selector.
	*
	* @returns A {@link SimpleSelector} to add to the current complex selector, or `false` to skip it.
	*/
	static convertSpecialAttributeSelectorAdgToUbo(name, value) {
		switch (name) {
			case AdgAttributeSelectors.TagContent: return HtmlRuleConverter.getPseudoClassSelectorNode(UboPseudoClasses.HasText, value);
			case AdgAttributeSelectors.Wildcard: return HtmlRuleConverter.getPseudoClassSelectorNode(UboPseudoClasses.HasText, RegExpUtils.globToRegExp(value));
			case AdgAttributeSelectors.MinLength:
				HtmlRuleConverter.assertValidLengthValue(name, value, ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_VALUE_INT, ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_VALUE_POSITIVE);
				return HtmlRuleConverter.getPseudoClassSelectorNode(UboPseudoClasses.MinTextLength, value);
			case AdgAttributeSelectors.MaxLength: return false;
			default: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_NOT_SUPPORTED, name));
		}
	}
	/**
	* Handles special pseudo-class selectors during AdGuard to uBlock conversion:
	* - `:contains(text)` -> `:has-text(text)`
	*   direct conversion, no changes to argument
	*
	* @param name Name of the special pseudo-class selector.
	* @param argument Argument of the special pseudo-class selector.
	*
	* @returns A {@link SimpleSelector} to add to the current complex selector.
	*
	* @throws If uBlock-specific pseudo-class selector is found in AdGuard rule.
	*/
	static convertSpecialPseudoClassSelectorAdgToUbo(name, argument) {
		switch (name) {
			case AdgPseudoClasses.Contains: return HtmlRuleConverter.getPseudoClassSelectorNode(UboPseudoClasses.HasText, argument);
			case UboPseudoClasses.HasText:
			case UboPseudoClasses.MinTextLength: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, ERROR_MESSAGES.MIXED_SYNTAX_ADG_UBO));
			default: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_PSEUDO_CLASS_SELECTOR_NOT_SUPPORTED, name));
		}
	}
	/**
	* Converts a HTML filtering rule body by handling special simple selectors via callbacks.
	* Special simple selectors are skipped in the converted selector list and should be handled from callee.
	*
	* @param body HTML filtering rule body to convert.
	* @param parser HTML filtering rule body parser used for parsing raw value bodies.
	* @param generator HTML filtering rule body generator used for generating raw value bodies.
	* @param onSpecialAttributeSelector Callback invoked when a special attribute selector is found.
	* @param onSpecialPseudoClassSelector Callback invoked when a special pseudo-class selector is found.
	*
	* @returns Converted selector list without special simple selectors.
	*/
	static convertBody(body, parser, generator, onSpecialAttributeSelector, onSpecialPseudoClassSelector) {
		let processedBody;
		if (body.type === "Value") processedBody = parser.parse(body.value, {
			isLocIncluded: false,
			parseHtmlFilteringRuleBodies: true
		});
		else processedBody = body;
		const { children: complexSelectors } = processedBody.selectorList;
		HtmlRuleConverter.assertNotEmpty(complexSelectors, ERROR_MESSAGES.EMPTY_SELECTOR_LIST);
		const convertedComplexSelectors = [];
		for (let i = 0; i < complexSelectors.length; i += 1) {
			const { children: selectors } = complexSelectors[i];
			HtmlRuleConverter.assertNotEmpty(selectors, ERROR_MESSAGES.EMPTY_COMPLEX_SELECTOR);
			const convertedSelectors = [];
			for (let j = 0; j < selectors.length; j += 1) {
				const selector = selectors[j];
				switch (selector.type) {
					case "SelectorCombinator":
						if (j === 0 || j === selectors.length - 1 || j > 0 && selectors[j - 1].type === "SelectorCombinator") throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, (0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_SELECTOR_COMBINATOR, selector.value)));
						break;
					case "AttributeSelector": {
						if (!SUPPORTED_ADG_ATTRIBUTE_SELECTORS.has(selector.name.value)) break;
						if (!("value" in selector) || selector.value.value === "") throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_VALUE_REQUIRED, selector.name.value));
						if (selector.operator.value !== "=") throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_OPERATOR_INVALID, selector.name.value, selector.operator.value));
						if (selector.flag) throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_ATTRIBUTE_SELECTOR_FLAG_NOT_SUPPORTED, selector.name.value));
						const name = selector.name.value;
						const { value } = selector.value;
						const result = onSpecialAttributeSelector(name, value);
						if (typeof result !== "boolean") {
							convertedSelectors.push(result);
							continue;
						} else if (result === false) continue;
						break;
					}
					case "PseudoClassSelector": {
						if (!SUPPORTED_ADG_PSEUDO_CLASSES.has(selector.name.value) && !SUPPORTED_UBO_PSEUDO_CLASSES.has(selector.name.value)) break;
						if (!selector.argument || selector.argument.value === "") throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SPECIAL_PSEUDO_CLASS_SELECTOR_ARGUMENT_REQUIRED, selector.name.value));
						const name = selector.name.value;
						const argument = selector.argument.value;
						const result = onSpecialPseudoClassSelector(name, argument);
						if (typeof result !== "boolean") {
							convertedSelectors.push(result);
							continue;
						} else if (result === false) continue;
						break;
					}
				}
				convertedSelectors.push(HtmlRuleConverter.cloneSelector(selector));
			}
			convertedComplexSelectors.push({
				type: "ComplexSelector",
				children: convertedSelectors
			});
		}
		let convertedBody = {
			type: "HtmlFilteringRuleBody",
			selectorList: {
				type: "SelectorList",
				children: convertedComplexSelectors
			}
		};
		if (body.type === "Value") convertedBody = {
			type: "Value",
			value: generator.generate(convertedBody)
		};
		return convertedBody;
	}
	/**
	* Clones a simple selector or selector combinator node.
	*
	* @param selector Simple selector or selector combinator node to clone.
	*
	* @returns Cloned simple selector or selector combinator node.
	*/
	static cloneSelector(selector) {
		const { type } = selector;
		switch (type) {
			case "TypeSelector":
			case "IdSelector":
			case "ClassSelector": return {
				type: selector.type,
				value: selector.value
			};
			case "SelectorCombinator": return {
				type: selector.type,
				value: selector.value
			};
			case "AttributeSelector": {
				const attributeSelectorClone = {
					type: selector.type,
					name: {
						type: selector.name.type,
						value: selector.name.value
					}
				};
				if ("value" in selector && selector.value) {
					attributeSelectorClone.operator = {
						type: selector.operator.type,
						value: selector.operator.value
					};
					attributeSelectorClone.value = {
						type: selector.value.type,
						value: selector.value.value
					};
					if (selector.flag) attributeSelectorClone.flag = {
						type: selector.flag.type,
						value: selector.flag.value
					};
				}
				return attributeSelectorClone;
			}
			case "PseudoClassSelector": {
				const pseudoClassSelectorClone = {
					type: selector.type,
					name: {
						type: selector.name.type,
						value: selector.name.value
					}
				};
				if (selector.argument) pseudoClassSelectorClone.argument = {
					type: selector.argument.type,
					value: selector.argument.value
				};
				return pseudoClassSelectorClone;
			}
			default: throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, (0, import_sprintf.sprintf)(ERROR_MESSAGES.UNKNOWN_SELECTOR_TYPE, type)));
		}
	}
	/**
	* Creates a CSS pseudo-class selector node.
	*
	* @param name The name of the pseudo-class selector.
	* @param argument Optional argument of the pseudo-class selector.
	*
	* @returns CSS pseudo-class selector node.
	*/
	static getPseudoClassSelectorNode(name, argument) {
		return {
			type: "PseudoClassSelector",
			name: {
				type: "Value",
				value: name
			},
			argument: argument ? {
				type: "Value",
				value: argument
			} : void 0
		};
	}
	/**
	* Asserts that the given array is not empty.
	*
	* @param array Array to check.
	* @param errorMessage Error message to use if the array is empty.
	*
	* @throws If the array is empty.
	*/
	static assertNotEmpty(array, errorMessage) {
		if (array.length === 0) throw new RuleConversionError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_RULE, errorMessage));
	}
	/**
	* Asserts that the given special attribute / pseudo-class length value is valid.
	*
	* @param name Name of the attribute or pseudo-class.
	* @param value Value to parse.
	* @param notIntErrorMessage Error message when the value is not an integer.
	* @param notPositiveErrorMessage Error message when the value is not positive.
	*
	* @throws If the value is not a valid number or not positive.
	*/
	static assertValidLengthValue(name, value, notIntErrorMessage, notPositiveErrorMessage) {
		const parsed = Number(value);
		if (Number.isNaN(parsed)) throw new RuleConversionError((0, import_sprintf.sprintf)(notIntErrorMessage, name, value));
		if (parsed < 0) throw new RuleConversionError((0, import_sprintf.sprintf)(notPositiveErrorMessage, name, value));
	}
};
//#endregion
export { HtmlRuleConverter };
