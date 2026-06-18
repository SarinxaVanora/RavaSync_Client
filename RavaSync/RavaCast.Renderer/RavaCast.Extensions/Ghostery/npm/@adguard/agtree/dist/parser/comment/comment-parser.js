import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { AgentCommentParser } from "./agent-comment-parser.js";
import { ConfigCommentParser } from "./config-comment-parser.js";
import { HintCommentParser } from "./hint-comment-parser.js";
import { MetadataCommentParser } from "./metadata-comment-parser.js";
import { PreProcessorCommentParser } from "./preprocessor-parser.js";
import { SimpleCommentParser } from "./simple-comment-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/comment-parser.js
/**
* `CommentParser` is responsible for parsing any comment-like adblock rules.
*
* @example
* Example rules:
*  - Adblock agent rules:
*      - ```adblock
*        [AdGuard]
*        ```
*      - ```adblock
*        [Adblock Plus 2.0]
*        ```
*      - etc.
*  - AdGuard hint rules:
*      - ```adblock
*        !+ NOT_OPTIMIZED
*        ```
*      - ```adblock
*        !+ NOT_OPTIMIZED PLATFORM(windows)
*        ```
*      - etc.
*  - Pre-processor rules:
*      - ```adblock
*        !#if (adguard)
*        ```
*      - ```adblock
*        !#endif
*        ```
*      - etc.
*  - Metadata rules:
*      - ```adblock
*        ! Title: My List
*        ```
*      - ```adblock
*        ! Version: 2.0.150
*        ```
*      - etc.
*  - AGLint inline config rules:
*      - ```adblock
*        ! aglint-enable some-rule
*        ```
*      - ```adblock
*        ! aglint-disable some-rule
*        ```
*      - etc.
*  - Simple comments:
*      - Regular version:
*        ```adblock
*        ! This is just a comment
*        ```
*      - uBlock Origin / "hostlist" version:
*        ```adblock
*        # This is just a comment
*        ```
*      - etc.
*/
var CommentParser = class CommentParser extends BaseParser {
	/**
	* Checks whether a rule is a comment.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is a comment, `false` otherwise
	*/
	static isCommentRule(raw) {
		const trimmed = raw.trim();
		return SimpleCommentParser.isSimpleComment(trimmed) || AgentCommentParser.isAgentRule(trimmed);
	}
	/**
	* Parses a raw rule as comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Comment AST or null (if the raw rule cannot be parsed as comment)
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (!CommentParser.isCommentRule(raw)) return null;
		return AgentCommentParser.parse(raw, options, baseOffset) || HintCommentParser.parse(raw, options, baseOffset) || PreProcessorCommentParser.parse(raw, options, baseOffset) || MetadataCommentParser.parse(raw, options, baseOffset) || ConfigCommentParser.parse(raw, options, baseOffset) || SimpleCommentParser.parse(raw, options, baseOffset);
	}
};
//#endregion
export { CommentParser };
