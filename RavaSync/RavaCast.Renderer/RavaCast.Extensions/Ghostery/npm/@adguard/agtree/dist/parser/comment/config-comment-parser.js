import { AdblockSyntax } from "../../utils/adblockers.js";
import "../../utils/constants.js";
import { StringUtils } from "../../utils/string.js";
import { CommentMarker, CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import lib from "../../../../../json5/dist/index.js";
import { ParameterListParser } from "../misc/parameter-list-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/config-comment-parser.js
/**
* @file AGLint configuration comments. Inspired by ESLint inline configuration comments.
* @see {@link https://eslint.org/docs/latest/user-guide/configuring/rules#using-configuration-comments}
*/
/**
* `ConfigCommentParser` is responsible for parsing inline AGLint configuration rules.
* Generally, the idea is inspired by ESLint inline configuration comments.
*
* @see {@link https://eslint.org/docs/latest/user-guide/configuring/rules#using-configuration-comments}
*/
var ConfigCommentParser = class ConfigCommentParser extends BaseParser {
	/**
	* Checks if the raw rule is an inline configuration comment rule.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is an inline configuration comment rule, otherwise `false`.
	*/
	static isConfigComment(raw) {
		const trimmed = raw.trim();
		if (trimmed[0] === CommentMarker.Regular || trimmed[0] === CommentMarker.Hashmark) {
			const text = raw.slice(1).trim();
			return (text[0] === "a" || text[0] === "A") && (text[1] === "g" || text[1] === "G") && (text[2] === "l" || text[2] === "L") && (text[3] === "i" || text[3] === "I") && (text[4] === "n" || text[4] === "N") && (text[5] === "t" || text[5] === "T");
		}
		return false;
	}
	/**
	* Parses a raw rule as an inline configuration comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns
	* Inline configuration comment AST or null (if the raw rule cannot be parsed as configuration comment)
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!ConfigCommentParser.isConfigComment(raw)) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		const marker = ValueParser.parse(raw[offset], options, baseOffset + offset);
		offset += 1;
		offset = StringUtils.skipWS(raw, offset);
		const commandStart = offset;
		offset = StringUtils.findNextWhitespaceCharacter(raw, offset);
		const command = ValueParser.parse(raw.slice(commandStart, offset), options, baseOffset + commandStart);
		offset = StringUtils.skipWS(raw, offset);
		const commentStart = raw.indexOf("--", offset);
		const commentEnd = commentStart !== -1 ? StringUtils.skipWSBack(raw) + 1 : -1;
		let comment;
		if (commentStart !== -1) comment = ValueParser.parse(raw.slice(commentStart, commentEnd), options, baseOffset + commentStart);
		const paramsStart = offset;
		const paramsEnd = commentStart !== -1 ? StringUtils.skipWSBack(raw, commentStart - 1) + 1 : StringUtils.skipWSBack(raw) + 1;
		let params;
		if (command.value === "aglint") {
			params = {
				type: "ConfigNode",
				value: lib.parse(`{${raw.slice(paramsStart, paramsEnd)}}`)
			};
			if (options.isLocIncluded) {
				params.start = paramsStart;
				params.end = paramsEnd;
			}
			if (Object.keys(params.value).length === 0) throw new Error("Empty AGLint config");
		} else if (paramsStart < paramsEnd) params = ParameterListParser.parse(raw.slice(paramsStart, paramsEnd), options, baseOffset + paramsStart, ",");
		const result = {
			type: CommentRuleType.ConfigCommentRule,
			category: RuleCategory.Comment,
			syntax: AdblockSyntax.Common,
			marker,
			command,
			params,
			comment
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
export { ConfigCommentParser };
