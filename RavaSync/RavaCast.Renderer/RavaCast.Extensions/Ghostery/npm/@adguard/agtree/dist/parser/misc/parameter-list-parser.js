import { StringUtils } from "../../utils/string.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "./value-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/parameter-list-parser.js
/**
* Parser for parameter lists.
*/
var ParameterListParser = class extends BaseParser {
	/**
	* Parses a raw parameter list.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @param separator Separator character (default: comma)
	* @returns Parameter list AST
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0, separator = ",") {
		const params = {
			type: "ParameterList",
			children: []
		};
		const { length } = raw;
		if (options.isLocIncluded) {
			params.start = baseOffset;
			params.end = baseOffset + length;
		}
		let offset = 0;
		while (offset < length) {
			offset = StringUtils.skipWS(raw, offset);
			if (raw[offset] === separator || offset === length) {
				params.children.push(null);
				offset += 1;
			} else {
				const paramStart = offset;
				const nextSeparator = StringUtils.findUnescapedNonStringNonRegexChar(raw, separator, offset);
				const paramEnd = nextSeparator !== -1 ? StringUtils.skipWSBack(raw, nextSeparator - 1) : StringUtils.skipWSBack(raw);
				const param = ValueParser.parse(raw.slice(paramStart, paramEnd + 1), options, baseOffset + paramStart);
				params.children.push(param);
				offset = nextSeparator !== -1 ? nextSeparator + 1 : length;
			}
		}
		if (raw[length - 1] === separator) params.children.push(null);
		return params;
	}
};
//#endregion
export { ParameterListParser };
