import { AbpSnippetInjectionBodyCommon } from "../../../common/abp-snippet-injection-body-common.js";
import { BaseGenerator } from "../../base-generator.js";
import { ParameterListGenerator } from "../../misc/parameter-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/scriptlet-body/abp-snippet-injection-body-generator.js
/**
* Adblock Plus snippet injection body generator.
*/
var AbpSnippetInjectionBodyGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the Adblock Plus-style snippet call body.
	*
	* @param node Scriptlet injection rule body
	* @returns String representation of the rule body
	* @throws Error if the scriptlet call is empty
	*/
	static generate(node) {
		const result = [];
		if (node.children.length === 0) throw new Error(AbpSnippetInjectionBodyCommon.ERROR_MESSAGES.EMPTY_SCRIPTLET_CALL);
		for (const scriptletCall of node.children) {
			if (scriptletCall.children.length === 0) throw new Error(AbpSnippetInjectionBodyCommon.ERROR_MESSAGES.EMPTY_SCRIPTLET_CALL);
			result.push(ParameterListGenerator.generate(scriptletCall, " "));
		}
		return result.join("; ");
	}
};
//#endregion
export { AbpSnippetInjectionBodyGenerator };
