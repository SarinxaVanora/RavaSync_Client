import { UBO_SCRIPTLET_MASK_LEGACY } from "../../../utils/constants.js";
import { StringUtils } from "../../../utils/string.js";
import { AdblockSyntaxError } from "../../../errors/adblock-syntax-error.js";
import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { UboParameterListParser } from "../../misc/ubo-parameter-list-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/scriptlet-body/ubo-scriptlet-injection-body-parser.js
/**
* @file uBlock scriptlet injection body parser
*/
/**
* `UboScriptletInjectionBodyParser` is responsible for parsing the body of a uBlock-style scriptlet rule.
*
* Please note that the parser will parse any scriptlet rule if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* example.com##+js(scriptlet0, arg0)
* ```
*
* but it didn't check if the scriptlet `scriptlet0` actually supported by any adblocker.
*
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#scriptlet-injection}
*/
var UboScriptletInjectionBodyParser = class extends BaseParser {
	/**
	* Error messages used by the parser.
	*/
	static ERROR_MESSAGES = {
		NO_SCRIPTLET_MASK: `Invalid uBO scriptlet call, no scriptlet call mask '+js' found`,
		NO_OPENING_PARENTHESIS: `Invalid uBO scriptlet call, no opening parentheses '(' found`,
		NO_CLOSING_PARENTHESIS: `Invalid uBO scriptlet call, no closing parentheses ')' found`,
		NO_SCRIPTLET_NAME: "Invalid uBO scriptlet call, no scriptlet name specified",
		WHITESPACE_AFTER_MASK: "Invalid uBO scriptlet call, whitespace is not allowed after the scriptlet call mask"
	};
	/**
	* Parses the body of a uBlock-style scriptlet rule.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Node of the parsed scriptlet call body
	* @throws If the body is syntactically incorrect
	* @example
	* ```
	* ##+js(scriptlet0, arg0)
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		let scriptletMaskLength = 0;
		if (raw.startsWith("+js", offset)) scriptletMaskLength = 3;
		else if (raw.startsWith("script:inject", offset)) scriptletMaskLength = UBO_SCRIPTLET_MASK_LEGACY.length;
		if (!scriptletMaskLength) throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_SCRIPTLET_MASK, baseOffset + offset, baseOffset + raw.length);
		offset += scriptletMaskLength;
		if (raw[offset] === " ") throw new AdblockSyntaxError(this.ERROR_MESSAGES.WHITESPACE_AFTER_MASK, baseOffset + offset, baseOffset + raw.length);
		if (raw[offset] !== "(") throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_OPENING_PARENTHESIS, baseOffset + offset, baseOffset + raw.length);
		const openingParenthesesIndex = offset;
		const closingParenthesesIndex = StringUtils.skipWSBack(raw, raw.length - 1);
		if (raw[closingParenthesesIndex] !== ")" || raw[closingParenthesesIndex - 1] === "\\") throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_CLOSING_PARENTHESIS, baseOffset + offset, baseOffset + raw.length);
		const result = {
			type: "ScriptletInjectionRuleBody",
			children: []
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		if (StringUtils.skipWS(raw, openingParenthesesIndex + 1) === closingParenthesesIndex) return result;
		const params = UboParameterListParser.parse(raw.slice(openingParenthesesIndex + 1, closingParenthesesIndex), options, baseOffset + openingParenthesesIndex + 1, ",");
		if (params.children.length > 0 && params.children[0] === null) throw new AdblockSyntaxError(this.ERROR_MESSAGES.NO_SCRIPTLET_NAME, baseOffset + offset, baseOffset + raw.length);
		result.children.push(params);
		return result;
	}
};
//#endregion
export { UboScriptletInjectionBodyParser };
