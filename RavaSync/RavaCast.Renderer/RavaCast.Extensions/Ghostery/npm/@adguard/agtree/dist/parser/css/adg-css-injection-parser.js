import { CSS_MEDIA_MARKER } from "../../utils/constants.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { TokenType } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { REMOVE_VALUE } from "../../converter/data/css.js";
import { CssTokenStream } from "./css-token-stream.js";
//#region node_modules/@adguard/agtree/dist/parser/css/adg-css-injection-parser.js
/**
* @file Parser for AdGuard CSS injections.
*/
var ERROR_MESSAGES = {
	MEDIA_QUERY_LIST_IS_EMPTY: "Media query list is empty",
	SELECTOR_LIST_IS_EMPTY: "Selector list is empty",
	DECLARATION_LIST_IS_EMPTY: "Declaration list is empty"
};
/**
* Parser for AdGuard CSS injection.
*/
var AdgCssInjectionParser = class extends BaseParser {
	/**
	* Parses an AdGuard CSS injection.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Parsed AdGuard CSS injection {@link CssInjectionRuleBody}.
	* @throws An {@link AdblockSyntaxError} if the selector list is syntactically invalid.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let mediaQueryList;
		const selectorList = {
			type: "Value",
			value: ""
		};
		const declarationList = {
			type: "Value",
			value: ""
		};
		const stream = new CssTokenStream(raw, baseOffset);
		stream.skipWhitespace();
		let balanceShift = 0;
		if (stream.getOrFail().type === TokenType.AtKeyword) {
			stream.expect(TokenType.AtKeyword, {
				value: CSS_MEDIA_MARKER,
				balance: 0
			});
			stream.advance();
			stream.skipWhitespace();
			const mediaQueryListStart = stream.getOrFail().start;
			let lastNonWsIndex = -1;
			while (!stream.isEof()) {
				const token = stream.getOrFail();
				if (token.type === TokenType.OpenCurlyBracket && token.balance === 1) break;
				if (token.type !== TokenType.Whitespace) lastNonWsIndex = token.end;
				stream.advance();
			}
			if (lastNonWsIndex === -1) throw new AdblockSyntaxError(ERROR_MESSAGES.MEDIA_QUERY_LIST_IS_EMPTY, baseOffset + mediaQueryListStart, baseOffset + raw.length);
			const mediaQueryListEnd = lastNonWsIndex;
			mediaQueryList = {
				type: "Value",
				value: raw.slice(mediaQueryListStart, mediaQueryListEnd)
			};
			if (options.isLocIncluded) {
				mediaQueryList.start = baseOffset + mediaQueryListStart;
				mediaQueryList.end = baseOffset + mediaQueryListEnd;
			}
			stream.expect(TokenType.OpenCurlyBracket);
			stream.advance();
			balanceShift = 1;
		}
		stream.skipWhitespace();
		const selectorStart = stream.getOrFail().start;
		const { skippedTrimmed: selectorTokensLength } = stream.skipUntilExt(TokenType.OpenCurlyBracket, balanceShift + 1);
		stream.expect(TokenType.OpenCurlyBracket);
		if (selectorTokensLength === 0) throw new AdblockSyntaxError(ERROR_MESSAGES.SELECTOR_LIST_IS_EMPTY, baseOffset + selectorStart, baseOffset + raw.length);
		const selectorEnd = stream.lookbehindForNonWs().end;
		selectorList.value = raw.slice(selectorStart, selectorEnd);
		if (options.isLocIncluded) {
			selectorList.start = baseOffset + selectorStart;
			selectorList.end = baseOffset + selectorEnd;
		}
		stream.advance();
		stream.skipWhitespace();
		const declarationsStart = stream.getOrFail().start;
		const declarations = /* @__PURE__ */ new Set();
		let declarationsEnd = -1;
		let remove = false;
		let lastNonWsIndex = -1;
		while (!stream.isEof()) {
			const token = stream.getOrFail();
			if (token.type === TokenType.CloseCurlyBracket && stream.getBalance() === balanceShift) {
				declarationsEnd = lastNonWsIndex;
				break;
			}
			if (token.type !== TokenType.Whitespace) lastNonWsIndex = token.end;
			if (token.type === TokenType.Ident && stream.lookahead()?.type === TokenType.Colon) {
				const ident = raw.slice(token.start, token.end);
				declarations.add(ident);
				stream.advance();
				stream.advance();
				if (ident === "remove") {
					stream.skipWhitespace();
					stream.expect(TokenType.Ident, { value: REMOVE_VALUE });
					stream.advance();
					remove = true;
				}
			} else stream.advance();
		}
		if (declarationsEnd === -1) throw new AdblockSyntaxError(ERROR_MESSAGES.DECLARATION_LIST_IS_EMPTY, baseOffset + declarationsStart, baseOffset + raw.length);
		declarationList.value = raw.slice(declarationsStart, declarationsEnd);
		if (options.isLocIncluded) {
			declarationList.start = baseOffset + declarationsStart;
			declarationList.end = baseOffset + declarationsEnd;
		}
		stream.expect(TokenType.CloseCurlyBracket);
		stream.advance();
		stream.skipWhitespace();
		if (balanceShift === 1) {
			stream.expect(TokenType.CloseCurlyBracket);
			stream.advance();
		}
		const result = {
			type: "CssInjectionRuleBody",
			selectorList,
			declarationList,
			remove
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		if (mediaQueryList) result.mediaQueryList = mediaQueryList;
		return result;
	}
};
//#endregion
export { AdgCssInjectionParser };
