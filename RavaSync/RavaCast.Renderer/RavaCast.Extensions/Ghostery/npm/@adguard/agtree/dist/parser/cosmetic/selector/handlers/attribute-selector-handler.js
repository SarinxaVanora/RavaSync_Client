import { AdblockSyntaxError } from "../../../../errors/adblock-syntax-error.js";
import { ValueParser } from "../../../misc/value-parser.js";
import { require_sprintf } from "../../../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../../../css-tokenizer/dist/csstokenizer.js";
import { QuoteUtils } from "../../../../utils/quotes.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/attribute-selector-handler.js
var import_sprintf = require_sprintf();
/**
* Handles attribute selector parsing in selector list.
*/
var AttributeSelectorHandler = class AttributeSelectorHandler {
	/**
	* Set of attribute equality prefixes.
	*
	* @see {@link AttributeSelectorOperatorValue}
	*/
	static ATTR_EQUALITY_PREFIXES = new Set([
		"~",
		"^",
		"$",
		"*",
		"|"
	]);
	/**
	* Set of valid flags for attribute selector values.
	*
	* @see {@link AttributeSelectorFlagValue}
	*/
	static ALLOWED_ATTRIBUTE_FLAGS = new Set(["i", "s"]);
	/**
	* Handles attribute selector parsing by creating an attribute selector node
	* and appending it to the current complex selector node.
	*
	* @param context Selector list parser context.
	*
	* @throws If the attribute selector is syntactically incorrect.
	*/
	static handle(context) {
		const { options, baseOffset, stream, complexSelector } = context;
		let token = stream.getOrFail();
		const { start } = token;
		stream.advance();
		stream.skipWhitespace();
		stream.expect(TokenType.Ident);
		token = stream.getOrFail();
		const nameRaw = stream.fragment();
		const result = {
			type: "AttributeSelector",
			name: ValueParser.parse(nameRaw, options, baseOffset + token.start)
		};
		if (options.isLocIncluded) result.start = baseOffset + start;
		stream.advance();
		stream.skipWhitespace();
		token = stream.getOrFail();
		if (token.type !== TokenType.CloseSquareBracket) {
			stream.expect(TokenType.Delim);
			let operatorRaw = stream.fragment();
			if (AttributeSelectorHandler.ATTR_EQUALITY_PREFIXES.has(operatorRaw)) {
				stream.advance();
				stream.expect(TokenType.Delim, { value: "=" });
				operatorRaw += "=";
			} else if (operatorRaw !== "=") throw new AdblockSyntaxError((0, import_sprintf.sprintf)("Invalid attribute selector operator '%s'", operatorRaw), baseOffset + token.start, baseOffset + token.end);
			const operatorStart = token.start;
			stream.advance();
			stream.skipWhitespace();
			token = stream.getOrFail();
			const isValueString = token.type === TokenType.String;
			if (!isValueString && token.type !== TokenType.Ident) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(`Expected '${getFormattedTokenName(TokenType.Ident)}' or '${getFormattedTokenName(TokenType.String)}' as attribute selector value, but got '%s' with value '%s'`, getFormattedTokenName(token.type), stream.fragment()), baseOffset + token.start, baseOffset + token.end);
			const valueStart = token.start + (isValueString ? 1 : 0);
			const valueEnd = token.end - (isValueString ? 1 : 0);
			const valueRaw = stream.fragment();
			const valueUnquotedAndUnescaped = QuoteUtils.removeQuotesAndUnescape(valueRaw);
			const operatorNode = {
				type: "Value",
				value: operatorRaw
			};
			const valueNode = {
				type: "Value",
				value: valueUnquotedAndUnescaped
			};
			result.operator = operatorNode;
			result.value = valueNode;
			if (options.isLocIncluded) {
				operatorNode.start = baseOffset + operatorStart;
				operatorNode.end = baseOffset + operatorStart + operatorRaw.length;
				valueNode.start = baseOffset + valueStart;
				valueNode.end = baseOffset + valueEnd;
			}
			stream.advance();
			stream.skipWhitespace();
			token = stream.getOrFail();
			if (token.type !== TokenType.CloseSquareBracket) {
				stream.expect(TokenType.Ident);
				const flagRaw = stream.fragment();
				if (!AttributeSelectorHandler.isValidFlag(flagRaw)) throw new AdblockSyntaxError((0, import_sprintf.sprintf)("Unexpected token '%s' with value '%s'", getFormattedTokenName(token.type), flagRaw), baseOffset + token.start, baseOffset + token.end);
				const flagStart = token.start;
				const flagNode = {
					type: "Value",
					value: flagRaw
				};
				result.flag = flagNode;
				if (options.isLocIncluded) {
					flagNode.start = baseOffset + flagStart;
					flagNode.end = baseOffset + flagStart + flagRaw.length;
				}
				stream.advance();
				stream.skipWhitespace();
				token = stream.getOrFail();
			}
		}
		stream.expect(TokenType.CloseSquareBracket);
		if (options.isLocIncluded) result.end = baseOffset + token.end;
		complexSelector.children.push(result);
		stream.advance();
	}
	/**
	* Validates attribute selector flag.
	*
	* @param flag Attribute selector flag.
	*
	* @returns `true` if the flag is valid, otherwise `false`.
	*/
	static isValidFlag(flag) {
		return AttributeSelectorHandler.ALLOWED_ATTRIBUTE_FLAGS.has(flag);
	}
};
//#endregion
export { AttributeSelectorHandler };
