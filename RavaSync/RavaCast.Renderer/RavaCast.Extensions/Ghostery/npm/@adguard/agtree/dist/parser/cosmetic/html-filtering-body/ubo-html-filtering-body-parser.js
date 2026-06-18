import { UBO_RESPONSEHEADER_FN } from "../../../utils/constants.js";
import { AdblockSyntaxError } from "../../../errors/adblock-syntax-error.js";
import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { ValueParser } from "../../misc/value-parser.js";
import { require_sprintf } from "../../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../../css-tokenizer/dist/csstokenizer.js";
import { CssTokenStream } from "../../css/css-token-stream.js";
import { HtmlFilteringBodyParser } from "./html-filtering-body-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/html-filtering-body/ubo-html-filtering-body-parser.js
var import_sprintf = require_sprintf();
/**
* `UboHtmlFilteringBodyParser` is responsible for parsing the body of
* an uBlock-style HTML filtering rule, and also uBlock-style response header removal rule.
*
* Please note that the parser will parse any HTML filtering rule if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* example.com##^script:pseudo(content)
* example.com##^responseheader(header-name)
* ```
*
* but it didn't check if the pseudo selector `pseudo` or if
* the header name `header-name` actually supported by any adblocker.
*
* @see {@link https://www.w3.org/TR/selectors-4}
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#html-filters}
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#response-header-filtering}
*/
var UboHtmlFilteringBodyParser = class UboHtmlFilteringBodyParser extends BaseParser {
	/**
	* Parses the body of an uBlock-style HTML filtering rule
	* and also uBlock-style response header removal rule.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Node of the parsed HTML filtering rule body.
	*
	* @throws If the body is syntactically incorrect.
	*
	* @example
	* ```
	* div:has-text(Example)
	* responseheader(header-name)
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const responseHeaderBody = UboHtmlFilteringBodyParser.parseResponseHeaderRule(raw, options, baseOffset);
		if (responseHeaderBody !== null) return responseHeaderBody;
		return HtmlFilteringBodyParser.parse(raw, options, baseOffset);
	}
	/**
	* Parses uBlock-style response header removal rule body.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Node of the parsed response header removal rule body
	* or `null` if the body is not a response header removal rule.
	*
	* @throws If the body is syntactically incorrect.
	*
	* @example
	* ```
	* responseheader(header-name)
	* ```
	*
	* @note This method returns `HtmlFilteringRuleBody` because,
	* response header removal rule syntax is same as uBlock-style
	* HTML filtering rule syntax.
	*/
	static parseResponseHeaderRule(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!options.parseHtmlFilteringRuleBodies) return null;
		const stream = new CssTokenStream(raw, baseOffset);
		stream.skipWhitespace();
		let token = stream.get();
		if (!token || token.type !== TokenType.Function) return null;
		const { start } = token;
		const functionNameRaw = raw.slice(token.start, token.end - 1);
		if (functionNameRaw !== "responseheader") return null;
		stream.advance();
		stream.skipWhitespace();
		token = stream.getOrFail();
		const argumentStart = token.start;
		stream.skipUntilBalanced();
		token = stream.getOrFail();
		const argumentEnd = token.start;
		const argumentRaw = raw.slice(argumentStart, argumentEnd).trimEnd();
		if (argumentRaw.length === 0) throw new AdblockSyntaxError(`Empty parameter for '${UBO_RESPONSEHEADER_FN}' function`, argumentStart + baseOffset, argumentEnd + baseOffset);
		stream.expect(TokenType.CloseParenthesis);
		stream.advance();
		stream.skipWhitespace();
		if (!stream.isEof()) {
			token = stream.getOrFail();
			throw new AdblockSyntaxError((0, import_sprintf.sprintf)("Expected end of rule, but got '%s'", getFormattedTokenName(token.type)), token.start + baseOffset, token.end + baseOffset);
		}
		const pseudoClassSelectorNode = {
			type: "PseudoClassSelector",
			name: ValueParser.parse(functionNameRaw, options, start + baseOffset),
			argument: ValueParser.parse(argumentRaw, options, argumentStart + baseOffset)
		};
		const complexSelectorNode = {
			type: "ComplexSelector",
			children: [pseudoClassSelectorNode]
		};
		const selectorList = {
			type: "SelectorList",
			children: [complexSelectorNode]
		};
		const result = {
			type: "HtmlFilteringRuleBody",
			selectorList
		};
		const lastNonWsToken = stream.lookbehindForNonWs();
		if (!lastNonWsToken) return null;
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = raw.length + baseOffset;
			selectorList.start = start + baseOffset;
			selectorList.end = lastNonWsToken.end + baseOffset;
			complexSelectorNode.start = selectorList.start;
			complexSelectorNode.end = selectorList.end;
			pseudoClassSelectorNode.start = complexSelectorNode.start;
			pseudoClassSelectorNode.end = complexSelectorNode.end;
		}
		return result;
	}
};
//#endregion
export { UboHtmlFilteringBodyParser };
