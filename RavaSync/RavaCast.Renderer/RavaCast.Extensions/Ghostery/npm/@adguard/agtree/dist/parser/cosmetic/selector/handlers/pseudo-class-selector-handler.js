import { AdblockSyntaxError } from "../../../../errors/adblock-syntax-error.js";
import { ValueParser } from "../../../misc/value-parser.js";
import { require_sprintf } from "../../../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../../../css-tokenizer/dist/csstokenizer.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/pseudo-class-selector-handler.js
var import_sprintf = require_sprintf();
/**
* Handles pseudo-class selector parsing in selector list.
*/
var PseudoClassSelectorHandler = class {
	/**
	* Handles pseudo-class selector parsing by creating a pseudo-class selector node
	* and appending it to the current complex selector node.
	*
	* @param context Selector list parser context.
	*
	* @throws If the pseudo-class selector is syntactically incorrect.
	*/
	static handle(context) {
		const { raw, options, baseOffset, stream, complexSelector } = context;
		let token = stream.getOrFail();
		const { start } = token;
		stream.advance();
		token = stream.getOrFail();
		const isFunction = token.type === TokenType.Function;
		if (!isFunction && token.type !== TokenType.Ident) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(`Expected '${getFormattedTokenName(TokenType.Ident)}' or '${getFormattedTokenName(TokenType.Function)}' as pseudo-class selector name, but got '%s' with value '%s'`, getFormattedTokenName(token.type), stream.fragment()), baseOffset + token.start, baseOffset + token.end);
		let nameRaw;
		if (!isFunction) nameRaw = raw.slice(token.start, token.end);
		else nameRaw = raw.slice(token.start, token.end - 1);
		const result = {
			type: "PseudoClassSelector",
			name: ValueParser.parse(nameRaw, options, baseOffset + token.start)
		};
		if (options.isLocIncluded) result.start = baseOffset + start;
		if (isFunction) {
			stream.advance();
			token = stream.getOrFail();
			result.argument = {
				type: "Value",
				value: ""
			};
			if (options.isLocIncluded) {
				result.argument.start = baseOffset + token.start;
				result.argument.end = baseOffset + token.start;
			}
			stream.skipWhitespace();
			token = stream.getOrFail();
			if (token.type !== TokenType.CloseParenthesis) {
				const balance = stream.getBalance();
				stream.skipWhitespace();
				token = stream.getOrFail();
				const argumentStart = token.start;
				let lastNonWsToken;
				while (stream.get()?.balance !== balance - 1) {
					const currentToken = stream.get();
					if (currentToken && currentToken.type !== TokenType.Whitespace) lastNonWsToken = currentToken;
					stream.advance();
				}
				token = stream.getOrFail();
				const argumentEnd = lastNonWsToken ? lastNonWsToken.end : token.start;
				const argumentRaw = raw.slice(argumentStart, argumentEnd);
				result.argument.value = argumentRaw;
				if (options.isLocIncluded) {
					result.argument.start = baseOffset + argumentStart;
					result.argument.end = baseOffset + argumentStart + argumentRaw.length;
				}
			}
			stream.expect(TokenType.CloseParenthesis);
		}
		if (options.isLocIncluded) result.end = baseOffset + token.end;
		complexSelector.children.push(result);
		stream.advance();
	}
};
//#endregion
export { PseudoClassSelectorHandler };
