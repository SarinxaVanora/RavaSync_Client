import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { HintParser } from "./hint-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/hint-comment-parser.js
/**
* `HintRuleParser` is responsible for parsing AdGuard hint rules.
*
* @example
* The following hint rule
* ```adblock
* !+ NOT_OPTIMIZED PLATFORM(windows)
* ```
* contains two hints: `NOT_OPTIMIZED` and `PLATFORM`.
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#hints}
*/
var HintCommentParser = class HintCommentParser extends BaseParser {
	/**
	* Checks if the raw rule is a hint rule.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is a hint rule, `false` otherwise
	*/
	static isHintRule(raw) {
		return raw.trim().startsWith("!+");
	}
	/**
	* Parses a raw rule as a hint comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Hint AST or null (if the raw rule cannot be parsed as a hint comment)
	* @throws If the input matches the HINT pattern but syntactically invalid
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#hints-1}
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!HintCommentParser.isHintRule(raw)) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw);
		offset += 2;
		const hints = [];
		while (offset < raw.length) {
			offset = StringUtils.skipWS(raw, offset);
			const hintStartIndex = offset;
			let hintEndIndex = offset;
			let balance = 0;
			while (hintEndIndex < raw.length) {
				if (raw[hintEndIndex] === "(" && raw[hintEndIndex - 1] !== "\\") {
					balance += 1;
					if (balance > 1) throw new AdblockSyntaxError("Invalid hint: nested parentheses are not allowed", baseOffset + hintStartIndex, baseOffset + hintEndIndex);
				} else if (raw[hintEndIndex] === ")" && raw[hintEndIndex - 1] !== "\\") balance -= 1;
				else if (StringUtils.isWhitespace(raw[hintEndIndex]) && balance === 0) break;
				hintEndIndex += 1;
			}
			offset = hintEndIndex;
			offset = StringUtils.skipWS(raw, offset);
			const hint = HintParser.parse(raw.slice(hintStartIndex, hintEndIndex), options, baseOffset + hintStartIndex);
			hints.push(hint);
		}
		if (hints.length === 0) throw new AdblockSyntaxError("Empty hint rule", baseOffset, baseOffset + offset);
		const result = {
			type: CommentRuleType.HintCommentRule,
			category: RuleCategory.Comment,
			syntax: AdblockSyntax.Adg,
			children: hints
		};
		if (options.includeRaws) result.raws = { text: raw };
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + offset;
		}
		return result;
	}
};
//#endregion
export { HintCommentParser };
