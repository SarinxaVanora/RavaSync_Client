import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { SelectorListParser } from "../selector/selector-list-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/html-filtering-body/html-filtering-body-parser.js
/**
* Class responsible for parsing HTML filtering rule body.
*
* Please note that the parser will parse any HTML filtering rule body if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* span[special-attr="Example"]
* div:special-pseudo(Example)
* ```
*
* but it didn't check if the pseudo selector `special-pseudo` or if
* the attribute selector `special-attr` actually supported by any adblocker.
*
* @see {@link https://www.w3.org/TR/selectors-4}
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#html-filtering-rules}
* @see {@link https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#html-filters}
*/
var HtmlFilteringBodyParser = class extends BaseParser {
	/**
	* Parses a HTML filtering rule body.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Node of the parsed HTML filtering rule body.
	*
	* @throws If the body is syntactically incorrect.
	*
	* @example
	* ```
	* span[tag-content="Example"]
	* div:has-text(Example)
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let result;
		if (options.parseHtmlFilteringRuleBodies) result = {
			type: "HtmlFilteringRuleBody",
			selectorList: SelectorListParser.parse(raw, options, baseOffset)
		};
		else result = {
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
export { HtmlFilteringBodyParser };
