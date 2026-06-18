import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { CommentMarker, CommentRuleType, RuleCategory } from "../../nodes/index.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/comment/metadata-comment-parser.js
/**
* @file Metadata comments
*/
/**
* Set of known metadata headers. This helps to quickly identify and validate
* metadata headers in the comments.
*/
var KNOWN_METADATA_HEADERS = new Set([
	"Checksum",
	"Description",
	"Expires",
	"Homepage",
	"Last Modified",
	"LastModified",
	"Licence",
	"License",
	"Time Updated",
	"TimeUpdated",
	"Version",
	"Title"
]);
/**
* `MetadataParser` is responsible for parsing metadata comments.
* Metadata comments are special comments that specify some properties of the list.
*
* @example
* For example, in the case of
* ```adblock
* ! Title: My List
* ```
* the name of the header is `Title`, and the value is `My List`, which means that
* the list title is `My List`, and it can be used in the adblocker UI.
* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#special-comments}
*/
var MetadataCommentParser = class extends BaseParser {
	/**
	* Parses a raw rule as a metadata comment.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Metadata comment AST or null (if the raw rule cannot be parsed as a metadata comment)
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		if (raw.indexOf(":") === -1) return null;
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		if (raw[offset] !== CommentMarker.Regular && raw[offset] !== CommentMarker.Hashmark) return null;
		const marker = ValueParser.parse(raw[offset], options, baseOffset + offset);
		offset += 1;
		offset = StringUtils.skipWS(raw, offset);
		const headerStart = offset;
		const text = raw.slice(offset);
		for (const knownHeader of KNOWN_METADATA_HEADERS) if (text.toLocaleLowerCase().startsWith(knownHeader.toLocaleLowerCase())) {
			offset += knownHeader.length;
			const header = ValueParser.parse(raw.slice(headerStart, offset), options, baseOffset + headerStart);
			offset = StringUtils.skipWS(raw, offset);
			if (raw[offset] !== ":") return null;
			offset += 1;
			offset = StringUtils.skipWS(raw, offset);
			const valueStart = offset;
			if (offset >= raw.length) return null;
			const valueEnd = StringUtils.skipWSBack(raw, raw.length - 1) + 1;
			const value = ValueParser.parse(raw.slice(valueStart, valueEnd), options, baseOffset + valueStart);
			const result = {
				type: CommentRuleType.MetadataCommentRule,
				category: RuleCategory.Comment,
				syntax: AdblockSyntax.Common,
				marker,
				header,
				value
			};
			if (options.includeRaws) result.raws = { text: raw };
			if (options.isLocIncluded) {
				result.start = baseOffset;
				result.end = baseOffset + raw.length;
			}
			return result;
		}
		return null;
	}
};
//#endregion
export { MetadataCommentParser };
