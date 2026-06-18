import { AdblockSyntaxError } from "../../../errors/adblock-syntax-error.js";
import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { require_sprintf } from "../../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../../css-tokenizer/dist/csstokenizer.js";
import { CssTokenStream } from "../../css/css-token-stream.js";
import { TypeSelectorHandler } from "./handlers/type-selector-handler.js";
import { IdSelectorHandler } from "./handlers/id-selector-handler.js";
import { AttributeSelectorHandler } from "./handlers/attribute-selector-handler.js";
import { PseudoClassSelectorHandler } from "./handlers/pseudo-class-selector-handler.js";
import { ClassSelectorHandler } from "./handlers/class-selector-handler.js";
import { CompoundSelectorHandler } from "./handlers/compound-selector-handler.js";
import { ComplexSelectorHandler } from "./handlers/complex-selector-handler.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/selector-list-parser.js
var import_sprintf = require_sprintf();
/**
* Class responsible for parsing selector lists.
*
* Please note that the parser will parse any selector list if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* div[attr1="value1"] > h1[attr2="value2"], span[attr3="value3"]
* ```
*
* but it didn't check if the given attribute or pseudo-class is valid or not.
*
* @see {@link https://www.w3.org/TR/selectors-4/#selector-list}'
*/
var SelectorListParser = class SelectorListParser extends BaseParser {
	/**
	* Common error messages used in the parser for unexpected tokens.
	*/
	static UNEXPECTED_TOKEN_WITH_VALUE_ERROR = "Unexpected token '%s' with value '%s'";
	/**
	* Parses a selector list.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Node of the parsed selector list.
	*
	* @throws If the selector list is syntactically incorrect.
	*
	* @example
	* ```
	* div[attr1="value1"] > h1[attr2="value2"], span[attr3="value3"]
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const stream = new CssTokenStream(raw, baseOffset);
		stream.skipWhitespace();
		const context = {
			raw,
			options,
			baseOffset,
			stream,
			token: stream.getOrFail(),
			result: {
				type: "SelectorList",
				children: []
			},
			complexSelector: {
				type: "ComplexSelector",
				children: []
			},
			isTypeSelectorSet: false
		};
		if (options.isLocIncluded) {
			context.result.start = baseOffset;
			context.result.end = baseOffset + raw.length;
			context.complexSelector.start = baseOffset + context.token.start;
		}
		while (!stream.isEof()) {
			context.token = stream.getOrFail();
			switch (context.token.type) {
				case TokenType.Ident:
					TypeSelectorHandler.handle(context);
					break;
				case TokenType.Hash:
					IdSelectorHandler.handle(context);
					break;
				case TokenType.OpenSquareBracket:
					AttributeSelectorHandler.handle(context);
					break;
				case TokenType.Colon:
					PseudoClassSelectorHandler.handle(context);
					break;
				case TokenType.Delim: {
					const delimiter = stream.fragment();
					switch (delimiter) {
						case "*":
							TypeSelectorHandler.handle(context);
							break;
						case ".":
							ClassSelectorHandler.handle(context);
							break;
						case ">":
						case "+":
						case "~":
							CompoundSelectorHandler.handle(context, delimiter);
							break;
						default: throw new AdblockSyntaxError((0, import_sprintf.sprintf)(SelectorListParser.UNEXPECTED_TOKEN_WITH_VALUE_ERROR, getFormattedTokenName(context.token.type), delimiter), baseOffset + context.token.start, baseOffset + context.token.end);
					}
					break;
				}
				case TokenType.Whitespace:
					CompoundSelectorHandler.handle(context, " ");
					break;
				case TokenType.Comma:
					ComplexSelectorHandler.handle(context, false);
					break;
				default: throw new AdblockSyntaxError((0, import_sprintf.sprintf)(SelectorListParser.UNEXPECTED_TOKEN_WITH_VALUE_ERROR, getFormattedTokenName(context.token.type), stream.fragment()), baseOffset + context.token.start, baseOffset + context.token.end);
			}
		}
		ComplexSelectorHandler.handle(context);
		return context.result;
	}
};
//#endregion
export { SelectorListParser };
