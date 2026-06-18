import { BaseGenerator } from "../../base-generator.js";
import { ValueGenerator } from "../../misc/value-generator.js";
import { SelectorListGenerator } from "../selector/selector-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/html-filtering-body/html-filtering-body-generator.js
/**
* HTML Filtering body generator.
*/
var HtmlFilteringBodyGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the HTML filtering rule body.
	*
	* @param node HTML filtering rule body.
	*
	* @returns String representation of the rule body.
	*
	* @throws Error if the rule body is invalid.
	*/
	static generate(node) {
		if (node.type === "Value") return ValueGenerator.generate(node);
		return SelectorListGenerator.generate(node.selectorList);
	}
};
//#endregion
export { HtmlFilteringBodyGenerator };
