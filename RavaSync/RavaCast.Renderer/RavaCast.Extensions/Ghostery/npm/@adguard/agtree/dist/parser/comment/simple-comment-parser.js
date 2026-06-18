import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { CommentMarker, CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { CosmeticRuleSeparatorUtils } from "../../utils/cosmetic-rule-separator.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/simple-comment-parser.js
/**
* `SimpleCommentParser` is responsible for parsing simple comments.
* Some comments have a special meaning in adblock syntax, like agent comments or hints,
* but this parser is only responsible for parsing regular comments,
* whose only purpose is to provide some human-readable information.
*
* @example
* ```adblock
* ! This is a simple comment
* # This is a simple comment, but in host-like syntax
* ```
*/
var SimpleCommentParser = class extends BaseParser {
	/**
	* Checks if the raw rule is a simple comment.
	*
	* @param raw Raw input to check.
	* @returns `true` if the input is a simple comment, `false` otherwise.
	* @note This method does not check for adblock agent comments.
	*/
	static isSimpleComment(raw) {
		const trimmed = raw.trim();
		if (trimmed.startsWith(CommentMarker.Regular)) return true;
		if (trimmed.startsWith(CommentMarker.Hashmark)) {
			const result = CosmeticRuleSeparatorUtils.find(trimmed);
			if (result === null) return true;
			const { end } = result;
			if (!trimmed[end] || StringUtils.isWhitespace(trimmed[end]) || trimmed[end] === CommentMarker.Hashmark && trimmed[end + 1] === CommentMarker.Hashmark) return true;
		}
		return false;
	}
	/**
	* Parses a raw rule as a simple comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Comment rule node or null (if the raw rule cannot be parsed as a simple comment).
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!this.isSimpleComment(raw)) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		const marker = ValueParser.parse(raw[offset], options, baseOffset + offset);
		offset += 1;
		const text = ValueParser.parse(raw.slice(offset), options, baseOffset + offset);
		const result = {
			category: RuleCategory.Comment,
			type: CommentRuleType.CommentRule,
			syntax: AdblockSyntax.Common,
			marker,
			text
		};
		if (options.includeRaws) result.raws = { text: raw };
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
};
//#endregion
export { SimpleCommentParser };
