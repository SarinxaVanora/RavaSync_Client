import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { isUndefined } from "../../utils/type-guards.js";
import { getAdblockSyntax } from "../../common/agent-common.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/agent-parser.js
/**
* `AgentParser` is responsible for parsing single adblock agent elements.
*
* @example
* If the adblock agent rule is
* ```adblock
* [Adblock Plus 2.0; AdGuard]
* ```
* then the adblock agents are `Adblock Plus 2.0` and `AdGuard`, and this
* class is responsible for parsing them. The rule itself is parsed by
* `AgentCommentParser`, which uses this class to parse single agents.
*/
var AgentParser = class AgentParser extends BaseParser {
	/**
	* Regex to match a version inside a string.
	*/
	static VERSION_REGEX = /\b\d+\.\d+(\.\d+)?\b/;
	/**
	* Checks if the string is a valid version.
	*
	* The string can have a version in formats like
	* [Adblock Plus 2.0], or [Adblock Plus 3.1; AdGuard].
	*
	* @param str String to check
	* @returns `true` if the string is a valid version, `false` otherwise
	*/
	static isValidVersion(str) {
		return AgentParser.VERSION_REGEX.test(str);
	}
	/**
	* Parses a raw rule as an adblock agent comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Agent rule AST
	* @throws {AdblockSyntaxError} If the raw rule cannot be parsed as an adblock agent
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		const nameStartIndex = offset;
		let nameEndIndex = offset;
		let name;
		let version;
		let syntax = AdblockSyntax.Common;
		while (offset < raw.length) {
			offset = StringUtils.skipWS(raw, offset);
			const partEnd = StringUtils.findNextWhitespaceCharacter(raw, offset);
			const part = raw.slice(offset, partEnd);
			if (AgentParser.isValidVersion(part)) {
				if (!isUndefined(version)) throw new AdblockSyntaxError("Duplicated versions are not allowed", baseOffset + offset, baseOffset + partEnd);
				const parsedNamePart = raw.slice(nameStartIndex, nameEndIndex);
				name = ValueParser.parse(parsedNamePart, options, baseOffset + nameStartIndex);
				version = ValueParser.parse(part, options, baseOffset + offset);
				syntax = getAdblockSyntax(parsedNamePart);
			} else nameEndIndex = partEnd;
			offset = StringUtils.skipWS(raw, partEnd);
		}
		if (isUndefined(name)) {
			const parsedNamePart = raw.slice(nameStartIndex, nameEndIndex);
			name = ValueParser.parse(parsedNamePart, options, baseOffset + nameStartIndex);
			syntax = getAdblockSyntax(parsedNamePart);
		}
		if (name.value.length === 0) throw new AdblockSyntaxError("Agent name cannot be empty", baseOffset, baseOffset + raw.length);
		const result = {
			type: "Agent",
			adblock: name,
			syntax
		};
		if (version) result.version = version;
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
};
//#endregion
export { AgentParser };
