import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/value-parser.js
/**
* Value parser.
* This parser is very simple, it just exists to provide a consistent interface for parsing.
*/
var ValueParser = class extends BaseParser {
	/**
	* Parses a value.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Value node.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const result = {
			type: "Value",
			value: raw
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
};
//#endregion
export { ValueParser };
