import "../../../../../virtual/_rolldown/runtime.js";
import { require_sprintf } from "../../../../sprintf-js/src/sprintf.js";
import "../../../css-tokenizer/dist/csstokenizer.js";
import "../../../../tldts/dist/es6/index.js";
import { RuleParser } from "../parser/rule-parser.js";
import { require_glob_to_regexp } from "../../../../glob-to-regexp/index.js";
import { RuleGenerator } from "../generator/rule-generator.js";
import { BaseConverter } from "./base-interfaces/base-converter.js";
import { createConversionResult } from "./base-interfaces/conversion-result.js";
import { RuleConverter } from "./rule.js";
require_glob_to_regexp();
require_sprintf();
/**
* @file Rule converter for raw rules
*
* Technically, this is a wrapper around `RuleConverter` that works with nodes instead of strings.
*/
/**
* Adblock filtering rule converter class.
*
* You can use this class to convert string-based adblock rules, since most of the converters work with nodes.
* This class just provides an extra layer on top of the {@link RuleConverter} and calls the parser/serializer
* before/after the conversion internally.
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var RawRuleConverter = class extends BaseConverter {
	/**
	* Converts an adblock filtering rule to AdGuard format, if possible.
	*
	* @param rawRule Raw rule text to convert
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the array of converted rule texts, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the original rule text will be returned
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rawRule) {
		const conversionResult = RuleConverter.convertToAdg(RuleParser.parse(rawRule));
		if (!conversionResult.isConverted) return createConversionResult([rawRule], false);
		return createConversionResult(conversionResult.result.map(RuleGenerator.generate), true);
	}
};
//#endregion
export { RawRuleConverter };
