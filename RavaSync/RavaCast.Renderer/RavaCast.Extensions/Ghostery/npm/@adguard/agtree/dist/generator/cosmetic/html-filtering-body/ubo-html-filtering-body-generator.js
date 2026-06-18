import { UBO_RESPONSEHEADER_FN } from "../../../utils/constants.js";
import { BaseGenerator } from "../../base-generator.js";
import { ValueGenerator } from "../../misc/value-generator.js";
import { HtmlFilteringBodyGenerator } from "./html-filtering-body-generator.js";
import { isUboResponseHeaderRemovalRuleBody } from "../../../common/ubo-html-filtering-body-common.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/html-filtering-body/ubo-html-filtering-body-generator.js
/**
* uBlock HTML Filtering body generator.
*/
var UboHtmlFilteringBodyGenerator = class UboHtmlFilteringBodyGenerator extends BaseGenerator {
	/**
	* Generates a string representation of the uBlock HTML filtering rule body
	* and also uBlock-style response header removal rules.
	*
	* @param node HTML filtering rule body.
	*
	* @returns String representation of the rule body.
	*
	* @throws Error if the rule body is invalid.
	*/
	static generate(node) {
		const responseHeaderBody = UboHtmlFilteringBodyGenerator.generateResponseHeaderRule(node);
		if (responseHeaderBody !== null) return responseHeaderBody;
		return HtmlFilteringBodyGenerator.generate(node);
	}
	/**
	* Generates a string representation of the uBlock-style response header removal rule.
	*
	* @param node Potential response header removal rule node.
	*
	* @returns String representation of the response header removal rule,
	* or `null` if the node is not a response header removal rule.
	*
	* @note This method accepts `HtmlFilteringRuleBody` as `node` because,
	* response header removal rule syntax is same as uBlock-style HTML filtering rule syntax.
	*/
	static generateResponseHeaderRule(node) {
		if (node.type !== "HtmlFilteringRuleBody" || !isUboResponseHeaderRemovalRuleBody(node)) return null;
		const { selectorList } = node;
		const pseudoClass = selectorList.children[0].children[0];
		const headerName = ValueGenerator.generate(pseudoClass.argument);
		const result = [];
		result.push(UBO_RESPONSEHEADER_FN);
		result.push("(");
		result.push(headerName);
		result.push(")");
		return result.join("");
	}
};
//#endregion
export { UboHtmlFilteringBodyGenerator };
