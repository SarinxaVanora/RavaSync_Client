import { NotImplementedError } from "../errors/not-implemented-error.js";
//#region node_modules/@adguard/agtree/dist/parser/base-parser.js
/**
* @file Base parser class.
*/
/**
* Base class for parsers. Each parser should extend this class.
*/
var BaseParser = class {
	/**
	* Parses the input string and returns the AST node.
	*
	* @param input Input string to parse.
	* @param options Parser options, see {@link ParserOptions}.
	* @param baseOffset Base offset. Locations in the AST node will be relative to this offset.
	* @param args Additional, parser-specific arguments, if needed.
	*/
	static parse(input, options, baseOffset, ...args) {
		throw new NotImplementedError();
	}
};
//#endregion
export { BaseParser };
