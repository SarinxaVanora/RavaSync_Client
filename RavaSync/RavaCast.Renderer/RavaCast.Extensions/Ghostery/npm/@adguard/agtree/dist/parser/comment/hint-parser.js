import { StringUtils } from "../../utils/string.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { ParameterListParser } from "../misc/parameter-list-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/hint-parser.js
/**
* @file AdGuard Hints
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#hints}
*/
/**
* `HintParser` is responsible for parsing AdGuard hints.
*
* @example
* If the hint rule is
* ```adblock
* !+ NOT_OPTIMIZED PLATFORM(windows)
* ```
* then the hints are `NOT_OPTIMIZED` and `PLATFORM(windows)`, and this
* class is responsible for parsing them. The rule itself is parsed by
* the `HintRuleParser`, which uses this class to parse single hints.
*/
var HintParser = class extends BaseParser {
	/**
	* Parses a raw rule as a hint.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Hint rule AST or null
	* @throws If the syntax is invalid
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		offset = StringUtils.skipWS(raw);
		const nameStartIndex = offset;
		for (; offset < raw.length; offset += 1) {
			const char = raw[offset];
			if (char === "(" || char === " ") break;
			if (!StringUtils.isAlphaNumeric(char) && char !== "_") throw new AdblockSyntaxError(`Invalid character "${char}" in hint name: "${char}"`, baseOffset + nameStartIndex, baseOffset + offset);
		}
		const nameEndIndex = offset;
		const name = raw.slice(nameStartIndex, nameEndIndex);
		if (name === "") throw new AdblockSyntaxError("Empty hint name", baseOffset, baseOffset + nameEndIndex);
		offset = StringUtils.skipWS(raw, offset);
		if (offset > nameEndIndex && raw[offset] === "(") throw new AdblockSyntaxError("Unexpected whitespace(s) between hint name and opening parenthesis", baseOffset + nameEndIndex, baseOffset + offset);
		const nameNode = ValueParser.parse(name, options, baseOffset + nameStartIndex);
		if (raw[offset] !== "(") {
			const result = {
				type: "Hint",
				name: nameNode
			};
			if (options.isLocIncluded) {
				result.start = baseOffset;
				result.end = baseOffset + offset;
			}
			return result;
		}
		offset += 1;
		const closeParenthesisIndex = raw.lastIndexOf(")");
		if (closeParenthesisIndex === -1) throw new AdblockSyntaxError(`Missing closing parenthesis for hint "${name}"`, baseOffset + nameStartIndex, baseOffset + raw.length);
		const paramsStartIndex = offset;
		const paramsEndIndex = closeParenthesisIndex;
		const params = ParameterListParser.parse(raw.slice(paramsStartIndex, paramsEndIndex), options, baseOffset + paramsStartIndex, ",");
		offset = closeParenthesisIndex + 1;
		offset = StringUtils.skipWS(raw, offset);
		if (offset !== raw.length) throw new AdblockSyntaxError(`Unexpected input after closing parenthesis for hint "${name}": "${raw.slice(closeParenthesisIndex + 1, offset + 1)}"`, baseOffset + closeParenthesisIndex + 1, baseOffset + offset + 1);
		const result = {
			type: "Hint",
			name: nameNode,
			params
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + offset;
		}
		return result;
	}
};
//#endregion
export { HintParser };
