import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { isNull } from "../../utils/type-guards.js";
import { AgentParser } from "./agent-parser.js";
import { CosmeticRuleSeparatorUtils } from "../../utils/cosmetic-rule-separator.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/agent-comment-parser.js
/**
* `AgentParser` is responsible for parsing an Adblock agent rules.
* Adblock agent comment marks that the filter list is supposed to
* be used by the specified ad blockers.
*
* @example
*  - ```adblock
*    [AdGuard]
*    ```
*  - ```adblock
*    [Adblock Plus 2.0]
*    ```
*  - ```adblock
*    [uBlock Origin]
*    ```
*  - ```adblock
*    [uBlock Origin 1.45.3]
*    ```
*  - ```adblock
*    [Adblock Plus 2.0; AdGuard]
*    ```
*/
var AgentCommentParser = class AgentCommentParser extends BaseParser {
	/**
	* Checks if the raw rule is an adblock agent comment.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is an adblock agent, `false` otherwise
	*/
	static isAgentRule(raw) {
		const rawTrimmed = raw.trim();
		if (rawTrimmed.startsWith("[") && rawTrimmed.endsWith("]")) return isNull(CosmeticRuleSeparatorUtils.find(rawTrimmed));
		return false;
	}
	/**
	* Parses a raw rule as an adblock agent comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Agent rule AST or null (if the raw rule cannot be parsed as an adblock agent comment)
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!AgentCommentParser.isAgentRule(raw)) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		offset += 1;
		const closingBracketIndex = StringUtils.skipWSBack(raw, raw.length - 1);
		if (closingBracketIndex === -1 || raw[closingBracketIndex] !== "]") throw new AdblockSyntaxError("Missing closing bracket", offset, offset + raw.length);
		const result = {
			type: CommentRuleType.AgentCommentRule,
			syntax: AdblockSyntax.Common,
			category: RuleCategory.Comment,
			children: []
		};
		if (options.includeRaws) result.raws = { text: raw };
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		while (offset < closingBracketIndex) {
			offset = StringUtils.skipWS(raw, offset);
			let separatorIndex = raw.indexOf(";", offset);
			if (separatorIndex === -1) separatorIndex = closingBracketIndex;
			const agentEndIndex = StringUtils.findLastNonWhitespaceCharacter(raw.slice(offset, separatorIndex)) + offset + 1;
			result.children.push(AgentParser.parse(raw.slice(offset, agentEndIndex), options, baseOffset + offset));
			offset = separatorIndex + 1;
		}
		if (result.children.length === 0) throw new AdblockSyntaxError("Empty agent list", baseOffset, baseOffset + raw.length);
		return result;
	}
};
//#endregion
export { AgentCommentParser };
