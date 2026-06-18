import { BaseParser } from "../../base-parser.js";
import { defaultParserOptions } from "../../options.js";
import { QuoteUtils } from "../../../utils/quotes.js";
import { HtmlFilteringBodyParser } from "./html-filtering-body-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/html-filtering-body/adg-html-filtering-body-parser.js
/**
* `AdgHtmlFilteringBodyParser` is responsible for parsing the body of an AdGuard-style HTML filtering rule.
*
* Please note that the parser will parse any HTML filtering rule if it is syntactically correct.
* For example, it will parse this:
* ```adblock
* example.com$$div[special-attr="value"]
* ```
*
* but it didn't check if the attribute `special-attr` actually supported by any adblocker.
*
* @see {@link https://www.w3.org/TR/selectors-4}
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#html-filtering-rules}
*/
var AdgHtmlFilteringBodyParser = class extends BaseParser {
	/**
	* Parses the body of an AdGuard-style HTML filtering rule.
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
	* div[some_attribute="some_value"]
	* ```
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const escapedRaw = QuoteUtils.escapeAttributeDoubleQuotes(raw);
		return HtmlFilteringBodyParser.parse(escapedRaw, options, baseOffset);
	}
};
//#endregion
export { AdgHtmlFilteringBodyParser };
