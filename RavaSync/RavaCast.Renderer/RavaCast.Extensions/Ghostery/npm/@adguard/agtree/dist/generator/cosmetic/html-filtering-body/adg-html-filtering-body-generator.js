import "../../../../../../../virtual/_rolldown/runtime.js";
import { require_sprintf } from "../../../../../../sprintf-js/src/sprintf.js";
import "../../../../../css-tokenizer/dist/csstokenizer.js";
import { QuoteUtils } from "../../../utils/quotes.js";
import "../../../../../../tldts/dist/es6/index.js";
import { BaseGenerator } from "../../base-generator.js";
import { require_glob_to_regexp } from "../../../../../../glob-to-regexp/index.js";
import { HtmlFilteringBodyGenerator } from "./html-filtering-body-generator.js";
require_glob_to_regexp();
require_sprintf();
/**
* AdGuard HTML Filtering body generator.
*/
var AdgHtmlFilteringBodyGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the AdGuard HTML filtering rule body.
	*
	* @param node HTML filtering rule body.
	*
	* @returns String representation of the rule body.
	*
	* @throws Error if the rule body is invalid.
	*/
	static generate(node) {
		const raw = HtmlFilteringBodyGenerator.generate(node);
		return QuoteUtils.unescapeAttributeDoubleQuotes(raw);
	}
};
//#endregion
export { AdgHtmlFilteringBodyGenerator };
