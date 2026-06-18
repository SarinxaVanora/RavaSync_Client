import { AdblockSyntax } from "../../utils/adblockers.js";
import { SAFARI_CB_AFFINITY } from "../../utils/constants.js";
import { StringUtils } from "../../utils/string.js";
import { CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { ParameterListParser } from "../misc/parameter-list-parser.js";
import { LogicalExpressionParser } from "../misc/logical-expression-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/preprocessor-parser.js
/**
* Pre-processor directives
*
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#pre-processor-directives}
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#pre-parsing-directives}
*/
/**
* `PreProcessorParser` is responsible for parsing preprocessor rules.
* Pre-processor comments are special comments that are used to control the behavior of the filter list processor.
* Please note that this parser only handles general syntax for now, and does not validate the parameters at
* the parsing stage.
*
* @example
* If your rule is
* ```adblock
* !#if (adguard)
* ```
* then the directive's name is `if` and its value is `(adguard)`, but the parameter list
* is not parsed / validated further.
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#pre-processor-directives}
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#pre-parsing-directives}
*/
var PreProcessorCommentParser = class PreProcessorCommentParser extends BaseParser {
	/**
	* Determines whether the rule is a pre-processor rule.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is a pre-processor rule, `false` otherwise
	*/
	static isPreProcessorRule(raw) {
		const trimmed = raw.trim();
		return trimmed.startsWith("!#") && trimmed[2] !== "#";
	}
	/**
	* Parses a raw rule as a pre-processor comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns
	* Pre-processor comment AST or null (if the raw rule cannot be parsed as a pre-processor comment)
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!PreProcessorCommentParser.isPreProcessorRule(raw)) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		offset += 2;
		offset = StringUtils.skipWS(raw, offset);
		const nameStart = offset;
		while (offset < raw.length) {
			const ch = raw[offset];
			if (ch === " " || ch === "(") break;
			offset += 1;
		}
		const nameEnd = offset;
		const name = ValueParser.parse(raw.slice(nameStart, nameEnd), options, baseOffset + nameStart);
		offset = StringUtils.skipWS(raw, offset);
		if (name.value === "safari_cb_affinity") {
			if (offset > nameEnd) throw new AdblockSyntaxError(`Unexpected whitespace after "${SAFARI_CB_AFFINITY}" directive name`, baseOffset + nameEnd, baseOffset + offset);
			if (StringUtils.skipWS(raw, offset) !== raw.length) {
				if (raw[offset] !== "(") throw new AdblockSyntaxError(`Unexpected character '${raw[offset]}' after '${SAFARI_CB_AFFINITY}' directive name`, baseOffset + offset, baseOffset + offset + 1);
				offset += 1;
				const parameterListStart = offset;
				const closingParenthesesIndex = StringUtils.skipWSBack(raw);
				if (closingParenthesesIndex === -1 || raw[closingParenthesesIndex] !== ")") throw new AdblockSyntaxError(`Missing closing parenthesis for '${SAFARI_CB_AFFINITY}' directive`, baseOffset + offset, baseOffset + raw.length);
				const parameterListEnd = closingParenthesesIndex;
				const result = {
					type: CommentRuleType.PreProcessorCommentRule,
					category: RuleCategory.Comment,
					syntax: AdblockSyntax.Adg,
					name,
					params: ParameterListParser.parse(raw.slice(parameterListStart, parameterListEnd), options, baseOffset + parameterListStart, ",")
				};
				if (options.includeRaws) result.raws = { text: raw };
				if (options.isLocIncluded) {
					result.start = baseOffset;
					result.end = baseOffset + raw.length;
				}
				return result;
			}
		}
		if (offset === raw.length) {
			if (name.value === "if" || name.value === "include") throw new AdblockSyntaxError(`Directive "${name.value}" requires parameters`, baseOffset, baseOffset + raw.length);
			const result = {
				type: CommentRuleType.PreProcessorCommentRule,
				category: RuleCategory.Comment,
				syntax: AdblockSyntax.Common,
				name
			};
			if (options.includeRaws) result.raws = { text: raw };
			if (options.isLocIncluded) {
				result.start = baseOffset;
				result.end = baseOffset + raw.length;
			}
			return result;
		}
		const paramsStart = offset;
		const paramsEnd = StringUtils.skipWSBack(raw) + 1;
		let params;
		if (name.value === "if") params = LogicalExpressionParser.parse(raw.slice(paramsStart, paramsEnd), options, baseOffset + paramsStart);
		else params = ValueParser.parse(raw.slice(paramsStart, paramsEnd), options, baseOffset + paramsStart);
		const result = {
			type: CommentRuleType.PreProcessorCommentRule,
			category: RuleCategory.Comment,
			syntax: AdblockSyntax.Common,
			name,
			params
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
export { PreProcessorCommentParser };
