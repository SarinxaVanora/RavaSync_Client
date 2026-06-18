import { CosmeticRuleSeparator } from "../nodes/index.js";
//#region node_modules/@adguard/agtree/dist/utils/cosmetic-rule-separator.js
/**
* @file Cosmetic rule separator finder and categorizer
*/
/**
* Utility class for cosmetic rule separators.
*/
var CosmeticRuleSeparatorUtils = class {
	/**
	* Checks whether the specified separator is an exception.
	*
	* @param separator Separator to check
	* @returns `true` if the separator is an exception, `false` otherwise
	*/
	static isException(separator) {
		return separator[1] === "@";
	}
	/**
	* Checks whether the specified separator is marks an Extended CSS cosmetic rule.
	*
	* @param separator Separator to check
	* @returns `true` if the separator is marks an Extended CSS cosmetic rule, `false` otherwise
	*/
	static isExtendedCssMarker(separator) {
		return separator === CosmeticRuleSeparator.ExtendedElementHiding || separator === CosmeticRuleSeparator.ExtendedElementHidingException || separator === CosmeticRuleSeparator.AdgExtendedCssInjection || separator === CosmeticRuleSeparator.AdgExtendedCssInjectionException;
	}
	/**
	* Looks for the cosmetic rule separator in the rule. This is a simplified version that
	* masks the recursive function.
	*
	* @param rule Raw rule
	* @returns Separator result or null if no separator was found
	*/
	static find(rule) {
		/**
		* Helper function to create results of the `find` method.
		*
		* @param start Start position
		* @param separator Separator type
		* @returns Cosmetic rule separator node
		*/
		function createResult(start, separator) {
			return {
				separator,
				start,
				end: start + separator.length
			};
		}
		for (let i = 0; i < rule.length; i += 1) {
			if (rule[i] === "#") {
				if (rule[i + 1] === "#" && rule[i - 1] !== " ") return createResult(i, CosmeticRuleSeparator.ElementHiding);
				if (rule[i + 1] === "?" && rule[i + 2] === "#") return createResult(i, CosmeticRuleSeparator.ExtendedElementHiding);
				if (rule[i + 1] === "%" && rule[i + 2] === "#") return createResult(i, CosmeticRuleSeparator.AdgJsInjection);
				if (rule[i + 1] === "$") {
					if (rule[i + 2] === "#") return createResult(i, CosmeticRuleSeparator.AdgCssInjection);
					if (rule[i + 2] === "?" && rule[i + 3] === "#") return createResult(i, CosmeticRuleSeparator.AdgExtendedCssInjection);
				}
				if (rule[i + 1] === "@") {
					if (rule[i + 2] === "#" && rule[i - 1] !== " ") return createResult(i, CosmeticRuleSeparator.ElementHidingException);
					if (rule[i + 2] === "?" && rule[i + 3] === "#") return createResult(i, CosmeticRuleSeparator.ExtendedElementHidingException);
					if (rule[i + 2] === "%" && rule[i + 3] === "#") return createResult(i, CosmeticRuleSeparator.AdgJsInjectionException);
					if (rule[i + 2] === "$") {
						if (rule[i + 3] === "#") return createResult(i, CosmeticRuleSeparator.AdgCssInjectionException);
						if (rule[i + 3] === "?" && rule[i + 4] === "#") return createResult(i, CosmeticRuleSeparator.AdgExtendedCssInjectionException);
					}
				}
			}
			if (rule[i] === "$") {
				if (rule[i + 1] === "$") return createResult(i, CosmeticRuleSeparator.AdgHtmlFiltering);
				if (rule[i + 1] === "@" && rule[i + 2] === "$") return createResult(i, CosmeticRuleSeparator.AdgHtmlFilteringException);
			}
		}
		return null;
	}
};
//#endregion
export { CosmeticRuleSeparatorUtils };
