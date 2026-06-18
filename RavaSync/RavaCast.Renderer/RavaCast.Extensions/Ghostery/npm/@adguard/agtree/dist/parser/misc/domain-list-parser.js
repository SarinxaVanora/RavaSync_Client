import { ListItemNodeType, ListNodeType } from "../../nodes/index.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ListItemsParser } from "./list-items-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/domain-list-parser.js
/**
* `DomainListParser` is responsible for parsing a domain list.
*
* @example
* - If the rule is `example.com,~example.net##.ads`, the domain list is `example.com,~example.net`.
* - If the rule is `ads.js^$script,domains=example.com|~example.org`, the domain list is `example.com|~example.org`.
* This parser is responsible for parsing these domain lists.
* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_domains}
*/
var DomainListParser = class extends BaseParser {
	/**
	* Parses a domain list, eg. `example.com,example.org,~example.org`
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @param separator Separator character (default: comma)
	*
	* @returns Domain list AST.
	* @throws An {@link AdblockSyntaxError} if the domain list is syntactically invalid.
	* @throws An {@link Error} if the options are invalid.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0, separator = ",") {
		if (separator !== "," && separator !== "|") throw new Error(`Invalid separator: ${separator}`);
		const result = {
			type: ListNodeType.DomainList,
			separator,
			children: ListItemsParser.parse(raw, options, baseOffset, separator, ListItemNodeType.Domain)
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
};
//#endregion
export { DomainListParser };
