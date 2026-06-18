import "../../../../../../virtual/_rolldown/runtime.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { LEGACY_EXT_CSS_ATTRIBUTE_PREFIX } from "../data/css.js";
import { CssTokenStream } from "../../parser/css/css-token-stream.js";
import { QuoteUtils } from "../../utils/quotes.js";
import "../../../../../tldts/dist/es6/index.js";
import { require_glob_to_regexp } from "../../../../../glob-to-regexp/index.js";
import { BaseConverter } from "../base-interfaces/base-converter.js";
import { createConversionResult } from "../base-interfaces/conversion-result.js";
//#region node_modules/@adguard/agtree/dist/converter/css/index.js
var import_sprintf = require_sprintf();
require_glob_to_regexp();
var ERROR_MESSAGES = { INVALID_ATTRIBUTE_VALUE: `Expected '${getFormattedTokenName(TokenType.Ident)}' or '${getFormattedTokenName(TokenType.String)}' as attribute value, but got '%s' with value '%s` };
var PseudoClasses = {
	AbpContains: "-abp-contains",
	AbpHas: "-abp-has",
	Contains: "contains",
	Has: "has",
	HasText: "has-text",
	MatchesCss: "matches-css",
	MatchesCssAfter: "matches-css-after",
	MatchesCssBefore: "matches-css-before"
};
var PseudoElements = {
	After: "after",
	Before: "before"
};
var PSEUDO_ELEMENT_NAMES = new Set([PseudoElements.After, PseudoElements.Before]);
/**
* CSS selector converter
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var CssSelectorConverter = class CssSelectorConverter extends BaseConverter {
	/**
	* Converts Extended CSS elements to AdGuard-compatible ones
	*
	* @param selectorList Selector list to convert
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the converted node, and its `isConverted` flag indicates whether the original node was converted.
	* If the node was not converted, the result will contain the original node with the same object reference
	* @throws If the rule is invalid or incompatible
	*/
	static convertToAdg(selectorList) {
		const stream = selectorList instanceof CssTokenStream ? selectorList : new CssTokenStream(selectorList);
		const converted = [];
		const convertAndPushPseudo = (pseudo) => {
			switch (pseudo) {
				case PseudoClasses.AbpContains:
				case PseudoClasses.HasText:
					converted.push(PseudoClasses.Contains);
					converted.push("(");
					break;
				case PseudoClasses.AbpHas:
					converted.push(PseudoClasses.Has);
					converted.push("(");
					break;
				case PseudoClasses.MatchesCssBefore:
				case PseudoClasses.MatchesCssAfter:
					converted.push(PseudoClasses.MatchesCss);
					converted.push("(");
					converted.push(pseudo.substring(PseudoClasses.MatchesCss.length + 1));
					converted.push(",");
					break;
				default:
					converted.push(pseudo);
					converted.push("(");
					break;
			}
		};
		while (!stream.isEof()) {
			const token = stream.getOrFail();
			if (token.type === TokenType.Colon) {
				stream.advance();
				converted.push(":");
				const tempToken = stream.getOrFail();
				if (tempToken.type === TokenType.Colon) {
					stream.advance();
					converted.push(":");
					continue;
				}
				if (tempToken.type === TokenType.Ident) {
					const name = stream.source.slice(tempToken.start, tempToken.end);
					if (PSEUDO_ELEMENT_NAMES.has(name)) {
						converted.push(":");
						converted.push(name);
					} else converted.push(name);
					stream.advance();
				} else if (tempToken.type === TokenType.Function) {
					convertAndPushPseudo(stream.source.slice(tempToken.start, tempToken.end - 1));
					stream.advance();
				}
			} else if (token.type === TokenType.OpenSquareBracket) {
				let tempToken;
				const { start } = token;
				stream.advance();
				stream.skipWhitespace();
				stream.expect(TokenType.Ident);
				tempToken = stream.getOrFail();
				let attr = stream.source.slice(tempToken.start, tempToken.end);
				if (!(attr.startsWith("-ext-") || attr.startsWith("-abp"))) {
					converted.push(stream.source.slice(start, tempToken.end));
					stream.advance();
					continue;
				}
				if (attr.startsWith("-ext-")) attr = attr.slice(LEGACY_EXT_CSS_ATTRIBUTE_PREFIX.length);
				stream.advance();
				stream.skipWhitespace();
				stream.expect(TokenType.Delim, { value: "=" });
				stream.advance();
				stream.skipWhitespace();
				tempToken = stream.getOrFail();
				if (tempToken.type !== TokenType.Ident && tempToken.type !== TokenType.String) throw new Error((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_ATTRIBUTE_VALUE, getFormattedTokenName(tempToken.type), stream.source.slice(tempToken.start, tempToken.end)));
				const value = stream.source.slice(tempToken.start, tempToken.end);
				stream.advance();
				stream.skipWhitespace();
				stream.expect(TokenType.CloseSquareBracket);
				stream.advance();
				converted.push(":");
				convertAndPushPseudo(attr);
				let processedValue = QuoteUtils.removeQuotes(value);
				if (attr === PseudoClasses.Has) processedValue = CssSelectorConverter.convertToAdg(processedValue).result;
				converted.push(processedValue);
				converted.push(")");
			} else {
				converted.push(stream.source.slice(token.start, token.end));
				stream.advance();
			}
		}
		const convertedSelectorList = converted.join("");
		return createConversionResult(convertedSelectorList, stream.source !== convertedSelectorList);
	}
};
//#endregion
export { CssSelectorConverter };
