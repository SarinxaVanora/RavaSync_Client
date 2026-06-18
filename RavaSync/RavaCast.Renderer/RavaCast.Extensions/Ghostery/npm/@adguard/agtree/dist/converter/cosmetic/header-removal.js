import { AdblockSyntax } from "../../utils/adblockers.js";
import { CosmeticRuleType, RuleCategory } from "../../nodes/index.js";
import { UboHtmlFilteringBodyParser } from "../../parser/cosmetic/html-filtering-body/ubo-html-filtering-body-parser.js";
import { isUboResponseHeaderRemovalRuleBody } from "../../common/ubo-html-filtering-body-common.js";
import { RuleConversionError } from "../../errors/rule-conversion-error.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { ADBLOCK_URL_SEPARATOR, ADBLOCK_URL_START } from "../../utils/regexp.js";
import { createModifierListNode, createModifierNode } from "../../ast-utils/modifiers.js";
import { createNetworkRuleNode } from "../../ast-utils/network-rules.js";
//#region node_modules/@adguard/agtree/dist/converter/cosmetic/header-removal.js
/**
* @file Converter for request header removal rules
*/
var ADG_REMOVEHEADER_MODIFIER = "removeheader";
var ERROR_MESSAGES = { MULTIPLE_DOMAINS_NOT_SUPPORTED: "Multiple domains are not supported yet" };
/**
* Converter for request header removal rules
*
* @todo Implement `convertToUbo` (ABP currently doesn't support header removal rules)
*/
var HeaderRemovalRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a header removal rule to AdGuard syntax, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	* @example
	* If the input rule is:
	* ```adblock
	* example.com##^responseheader(header-name)
	* ```
	* The output will be:
	* ```adblock
	* ||example.com^$removeheader=header-name
	* ```
	*/
	static convertToAdg(rule) {
		if (rule.category !== RuleCategory.Cosmetic || rule.type !== CosmeticRuleType.HtmlFilteringRule) return createNodeConversionResult([rule], false);
		let body = null;
		if (rule.body.type === "Value") body = UboHtmlFilteringBodyParser.parseResponseHeaderRule(rule.body.value, {
			isLocIncluded: false,
			parseHtmlFilteringRuleBodies: true
		});
		else body = rule.body;
		if (!body || !isUboResponseHeaderRemovalRuleBody(body)) return createNodeConversionResult([rule], false);
		const { selectorList } = body;
		const headerName = selectorList.children[0].children[0].argument.value;
		const pattern = [];
		if (rule.domains.children.length === 1) pattern.push(ADBLOCK_URL_START, rule.domains.children[0].value, ADBLOCK_URL_SEPARATOR);
		else if (rule.domains.children.length > 1) throw new RuleConversionError(ERROR_MESSAGES.MULTIPLE_DOMAINS_NOT_SUPPORTED);
		const modifiers = createModifierListNode();
		modifiers.children.push(createModifierNode(ADG_REMOVEHEADER_MODIFIER, headerName));
		return createNodeConversionResult([createNetworkRuleNode(pattern.join(""), modifiers, rule.exception, AdblockSyntax.Adg)], true);
	}
};
//#endregion
export { HeaderRemovalRuleConverter };
