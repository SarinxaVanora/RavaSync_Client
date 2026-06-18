import { ADG_SCRIPTLET_MASK } from "../../../utils/constants.js";
import { StringUtils } from "../../../utils/string.js";
import { AdblockSyntaxError } from "../../../errors/adblock-syntax-error.js";
import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { ValueParser } from "../../misc/value-parser.js";
import { isNull } from "../../../utils/type-guards.js";
import { require_sprintf } from "../../../../../../sprintf-js/src/sprintf.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/scriptlet-body/adg-scriptlet-injection-body-parser.js
var import_sprintf = require_sprintf();
/**
* @file AdGuard scriptlet injection body parser
*/
/**
* `AdgScriptletInjectionBodyParser` is responsible for parsing the body of an AdGuard-style scriptlet rule.
*
* Please note that the parser will parse any scriptlet rule if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* example.com#%#//scriptlet('scriptlet0', 'arg0')
* ```
*
* but it didn't check if the scriptlet `scriptlet0` actually supported by any adblocker.
*
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#scriptlets}
*/
var AdgScriptletInjectionBodyParser = class AdgScriptletInjectionBodyParser extends BaseParser {
	/**
	* Error messages used by the parser.
	*/
	static ERROR_MESSAGES = {
		NO_SCRIPTLET_MASK: `Invalid ADG scriptlet call, no scriptlet call mask '${ADG_SCRIPTLET_MASK}' found`,
		NO_OPENING_PARENTHESIS: `Invalid ADG scriptlet call, no opening parentheses '(' found`,
		NO_CLOSING_PARENTHESIS: `Invalid ADG scriptlet call, no closing parentheses ')' found`,
		WHITESPACE_AFTER_MASK: "Invalid ADG scriptlet call, whitespace is not allowed after the scriptlet call mask",
		NO_INCONSISTENT_QUOTES: "Invalid ADG scriptlet call, inconsistent quotes",
		NO_UNCLOSED_PARAMETER: "Invalid ADG scriptlet call, unclosed parameter",
		EXPECTED_QUOTE: "Invalid ADG scriptlet call, expected quote, got '%s'",
		EXPECTED_COMMA: "Invalid ADG scriptlet call, expected comma, got '%s'"
	};
	/**
	* Parses the body of an AdGuard-style scriptlet rule.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Node of the parsed scriptlet call body
	* @throws If the body is syntactically incorrect
	* @example
	* ```
	* //scriptlet('scriptlet0', 'arg0')
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		if (!raw.startsWith("//scriptlet", offset)) throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_SCRIPTLET_MASK, baseOffset + offset, baseOffset + raw.length);
		offset += ADG_SCRIPTLET_MASK.length;
		if (raw[offset] === " ") throw new AdblockSyntaxError(this.ERROR_MESSAGES.WHITESPACE_AFTER_MASK, baseOffset + offset, baseOffset + raw.length);
		if (raw[offset] !== "(") throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_OPENING_PARENTHESIS, baseOffset + offset, baseOffset + raw.length);
		const openingParenthesesIndex = offset;
		const closingParenthesesIndex = StringUtils.skipWSBack(raw, raw.length - 1);
		if (raw[closingParenthesesIndex] !== ")" || raw[closingParenthesesIndex - 1] === "\\") throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_CLOSING_PARENTHESIS, baseOffset + offset, baseOffset + raw.length);
		offset = StringUtils.skipWS(raw, offset + 1);
		const result = {
			type: "ScriptletInjectionRuleBody",
			children: []
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		if (StringUtils.skipWS(raw, openingParenthesesIndex + 1) === closingParenthesesIndex) return result;
		let detectedQuote = null;
		const parameterList = {
			type: "ParameterList",
			children: []
		};
		if (options.isLocIncluded) {
			parameterList.start = baseOffset + openingParenthesesIndex + 1;
			parameterList.end = baseOffset + closingParenthesesIndex;
		}
		while (offset < closingParenthesesIndex) {
			offset = StringUtils.skipWS(raw, offset);
			if (parameterList.children.length > 0) {
				if (raw[offset] !== ",") throw new AdblockSyntaxError((0, import_sprintf.sprintf)(AdgScriptletInjectionBodyParser.ERROR_MESSAGES.EXPECTED_COMMA, raw[offset]), baseOffset + offset, baseOffset + raw.length);
				offset += 1;
				offset = StringUtils.skipWS(raw, offset);
			}
			if (raw[offset] === "'" || raw[offset] === "\"") {
				if (isNull(detectedQuote)) detectedQuote = raw[offset];
				else if (detectedQuote !== raw[offset]) throw new AdblockSyntaxError(AdgScriptletInjectionBodyParser.ERROR_MESSAGES.NO_INCONSISTENT_QUOTES, baseOffset + offset, baseOffset + raw.length);
				const closingQuoteIndex = StringUtils.findNextUnescapedCharacter(raw, detectedQuote, offset + 1);
				if (closingQuoteIndex === -1) throw new AdblockSyntaxError(AdgScriptletInjectionBodyParser.ERROR_MESSAGES.NO_UNCLOSED_PARAMETER, baseOffset + offset, baseOffset + raw.length);
				const parameter = ValueParser.parse(raw.slice(offset, closingQuoteIndex + 1), options, baseOffset + offset);
				parameterList.children.push(parameter);
				offset = StringUtils.skipWS(raw, closingQuoteIndex + 1);
			} else throw new AdblockSyntaxError((0, import_sprintf.sprintf)(AdgScriptletInjectionBodyParser.ERROR_MESSAGES.EXPECTED_QUOTE, raw[offset]), baseOffset + offset, baseOffset + raw.length);
		}
		result.children.push(parameterList);
		return result;
	}
};
//#endregion
export { AdgScriptletInjectionBodyParser };
