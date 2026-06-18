import { StringUtils } from "../../utils/string.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "./value-parser.js";
import { ParameterListParser } from "./parameter-list-parser.js";
import { QUOTE_SET } from "../../utils/quotes.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/ubo-parameter-list-parser.js
/**
* Parser for uBO-specific parameter lists.
*/
var UboParameterListParser = class extends ParameterListParser {
	/**
	* Parses an "uBO-specific parameter list".
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @param separator Separator character (default: comma).
	* @param requireQuotes Whether to require quotes around the parameter values (default: false).
	* @param supportedQuotes Set of accepted quotes (default: {@link QUOTE_SET}).
	* @returns Parameter list node.
	*
	* @note Based on {@link https://github.com/gorhill/uBlock/blob/f9ab4b75041815e6e5690d80851189ae3dc660d0/src/js/static-filtering-parser.js#L607-L699} to provide consistency.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0, separator = ",", requireQuotes = false, supportedQuotes = QUOTE_SET) {
		const params = {
			type: "ParameterList",
			children: []
		};
		const { length } = raw;
		if (options.isLocIncluded) {
			params.start = baseOffset;
			params.end = baseOffset + length;
		}
		let offset = 0;
		let extraNull = false;
		while (offset < length) {
			offset = StringUtils.skipWS(raw, offset);
			const paramStart = offset;
			let paramEnd = offset;
			if (supportedQuotes.has(raw[offset])) {
				const possibleClosingQuoteIndex = StringUtils.findNextUnescapedCharacter(raw, raw[offset], offset + 1);
				if (possibleClosingQuoteIndex !== -1) {
					const nextSeparatorIndex = StringUtils.skipWS(raw, possibleClosingQuoteIndex + 1);
					if (nextSeparatorIndex === length) {
						paramEnd = StringUtils.skipWSBack(raw, length - 1) + 1;
						offset = length;
					} else if (raw[nextSeparatorIndex] === separator) {
						paramEnd = possibleClosingQuoteIndex + 1;
						offset = nextSeparatorIndex + 1;
					} else {
						if (requireQuotes) throw new AdblockSyntaxError(`Expected separator, got: '${raw[nextSeparatorIndex]}'`, baseOffset + nextSeparatorIndex, baseOffset + length);
						/**
						* At that point found `possibleClosingQuoteIndex` is wrong
						* | is `offset`
						* ~ is `possibleClosingQuoteIndex`
						* ^ is `nextSeparatorIndex`
						*
						* Example 1: "abc, ').cba='1'"
						*                  |      ~^
						* Example 2: "abc, ').cba, '1'"
						*                  |       ~^
						* Example 3: "abc, ').cba='1', cba"
						*                  |      ~^
						*
						* Search for separator before `possibleClosingQuoteIndex`
						*/
						const separatorIndexBeforeQuote = StringUtils.findNextUnescapedCharacterBackwards(raw, separator, possibleClosingQuoteIndex, "\\", offset + 1);
						if (separatorIndexBeforeQuote !== -1) {
							paramEnd = StringUtils.skipWSBack(raw, separatorIndexBeforeQuote - 1) + 1;
							offset = separatorIndexBeforeQuote + 1;
						} else {
							const separatorIndexAfterQuote = StringUtils.findNextUnescapedCharacter(raw, separator, possibleClosingQuoteIndex);
							if (separatorIndexAfterQuote !== -1) {
								paramEnd = StringUtils.skipWSBack(raw, separatorIndexAfterQuote - 1) + 1;
								offset = separatorIndexAfterQuote + 1;
							} else {
								paramEnd = StringUtils.skipWSBack(raw, length - 1) + 1;
								offset = length;
							}
						}
					}
				} else {
					if (requireQuotes) throw new AdblockSyntaxError("Expected closing quote, got end of string", baseOffset + offset, baseOffset + length);
					paramEnd = StringUtils.skipWSBack(raw, length - 1) + 1;
					offset = length;
				}
			} else {
				if (requireQuotes) throw new AdblockSyntaxError(`Expected quote, got: '${raw[offset]}'`, baseOffset + offset, baseOffset + length);
				const nextSeparator = StringUtils.findNextUnescapedCharacter(raw, separator, offset);
				if (nextSeparator === -1) {
					paramEnd = StringUtils.skipWSBack(raw, length - 1) + 1;
					offset = length;
				} else {
					paramEnd = StringUtils.skipWSBack(raw, nextSeparator - 1) + 1;
					offset = nextSeparator + 1;
					if (StringUtils.skipWS(raw, length - 1) === nextSeparator) extraNull = true;
				}
			}
			if (paramStart < paramEnd) params.children.push(ValueParser.parse(raw.slice(paramStart, paramEnd), options, baseOffset + paramStart));
			else params.children.push(null);
		}
		if (extraNull) params.children.push(null);
		return params;
	}
};
//#endregion
export { UboParameterListParser };
