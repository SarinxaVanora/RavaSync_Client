import { StringUtils } from "../../utils/string.js";
import { ListItemNodeType } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { defaultParserOptions } from "../options.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/list-items-parser.js
/**
* Prefixes for error messages which are used for parsing of value lists.
*/
var LIST_PARSE_ERROR_PREFIX = {
	EMPTY_ITEM: "Empty value specified in the list",
	NO_MULTIPLE_NEGATION: "Exception marker cannot be followed by another exception marker",
	NO_SEPARATOR_AFTER_NEGATION: "Exception marker cannot be followed by a separator",
	NO_SEPARATOR_AT_THE_BEGINNING: "Value list cannot start with a separator",
	NO_SEPARATOR_AT_THE_END: "Value list cannot end with a separator",
	NO_WHITESPACE_AFTER_NEGATION: "Exception marker cannot be followed by whitespace"
};
/**
* Parser for list items in modifiers.
*/
var ListItemsParser = class {
	/**
	* Parses a `raw` modifier value which may be represented as a list of items separated by `separator`.
	* Needed for $app, $denyallow, $domain, $method.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @param separator Separator character (default: comma)
	* @param type Type of the list items (default: {@link ListItemNodeType.Domain}).
	* @template T Type of the list items.
	*
	* @returns List of parsed items.
	* @throws An {@link AdblockSyntaxError} if the list is syntactically invalid
	*
	* @example
	* - parses an app list — `com.example.app|Example.exe`
	* - parses a domain list — `example.com,example.org,~example.org` or `example.com|~example.org`
	* - parses a method list — `~post|~put`
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0, separator = ",", type = ListItemNodeType.Unknown) {
		const rawListItems = [];
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		if (raw[offset] === separator) throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.NO_SEPARATOR_AT_THE_BEGINNING, baseOffset + offset, baseOffset + raw.length);
		const realEndIndex = StringUtils.skipWSBack(raw);
		if (raw[realEndIndex] === separator) throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.NO_SEPARATOR_AT_THE_END, baseOffset + realEndIndex, baseOffset + realEndIndex + 1);
		while (offset < raw.length) {
			offset = StringUtils.skipWS(raw, offset);
			let itemStart = offset;
			const separatorStartIndex = StringUtils.findUnescapedNonStringNonRegexChar(raw, separator, offset);
			const itemEnd = separatorStartIndex === -1 ? StringUtils.skipWSBack(raw) + 1 : StringUtils.skipWSBack(raw, separatorStartIndex - 1) + 1;
			const exception = raw[itemStart] === "~";
			if (exception) {
				itemStart += 1;
				const item = raw[itemStart];
				if (item === "~") throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.NO_MULTIPLE_NEGATION, baseOffset + itemStart, baseOffset + itemStart + 1);
				if (item === separator) throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.NO_SEPARATOR_AFTER_NEGATION, baseOffset + itemStart, baseOffset + itemStart + 1);
				if (StringUtils.isWhitespace(item)) throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.NO_WHITESPACE_AFTER_NEGATION, baseOffset + itemStart, baseOffset + itemStart + 1);
			}
			if (itemEnd <= itemStart) throw new AdblockSyntaxError(LIST_PARSE_ERROR_PREFIX.EMPTY_ITEM, baseOffset + itemStart, baseOffset + raw.length);
			const listItem = {
				type,
				value: raw.slice(itemStart, itemEnd),
				exception
			};
			if (options.isLocIncluded) {
				listItem.start = baseOffset + itemStart;
				listItem.end = baseOffset + itemEnd;
			}
			rawListItems.push(listItem);
			offset = separatorStartIndex === -1 ? raw.length : separatorStartIndex + 1;
		}
		return rawListItems;
	}
};
//#endregion
export { ListItemsParser };
