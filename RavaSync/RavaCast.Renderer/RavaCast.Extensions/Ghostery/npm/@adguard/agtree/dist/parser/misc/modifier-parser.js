import { StringUtils } from "../../utils/string.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "./value-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/modifier-parser.js
/**
* `ModifierParser` is responsible for parsing modifiers.
*
* @example
* `match-case`, `~third-party`, `domain=example.com|~example.org`
*/
var ModifierParser = class extends BaseParser {
	/**
	* Parses a modifier.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Parsed modifier
	* @throws An error if modifier name or value is empty.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		const modifierStart = offset;
		let exception = false;
		if (raw[offset] === "~") {
			offset += 1;
			exception = true;
		}
		offset = StringUtils.skipWS(raw, offset);
		const modifierNameStart = offset;
		const assignmentIndex = StringUtils.findNextUnescapedCharacter(raw, "=");
		const modifierEnd = Math.max(StringUtils.skipWSBack(raw) + 1, modifierNameStart);
		if (modifierNameStart === modifierEnd) throw new AdblockSyntaxError("Modifier name cannot be empty", baseOffset, baseOffset + raw.length);
		let modifier;
		let value;
		if (assignmentIndex === -1) modifier = ValueParser.parse(raw.slice(modifierNameStart, modifierEnd), options, baseOffset + modifierNameStart);
		else {
			const modifierNameEnd = StringUtils.skipWSBack(raw, assignmentIndex - 1) + 1;
			modifier = ValueParser.parse(raw.slice(modifierNameStart, modifierNameEnd), options, baseOffset + modifierNameStart);
			if (assignmentIndex + 1 === modifierEnd) throw new AdblockSyntaxError("Modifier value cannot be empty", baseOffset, baseOffset + raw.length);
			const valueStart = StringUtils.skipWS(raw, assignmentIndex + 1);
			value = ValueParser.parse(raw.slice(valueStart, modifierEnd), options, baseOffset + valueStart);
		}
		const result = {
			type: "Modifier",
			name: modifier,
			value,
			exception
		};
		if (options.isLocIncluded) {
			result.start = baseOffset + modifierStart;
			result.end = baseOffset + modifierEnd;
		}
		return result;
	}
};
//#endregion
export { ModifierParser };
