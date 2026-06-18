import { ADG_SCRIPTLET_MASK } from "../../../utils/constants.js";
import { BaseGenerator } from "../../base-generator.js";
import { ParameterListGenerator } from "../../misc/parameter-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/scriptlet-body/adg-scriptlet-injection-body-generator.js
/**
* AdGuard scriptlet injection body generator.
*/
var AdgScriptletInjectionBodyGenerator = class AdgScriptletInjectionBodyGenerator extends BaseGenerator {
	/**
	* Error messages used by the generator.
	*/
	static ERROR_MESSAGES = { NO_MULTIPLE_SCRIPTLET_CALLS: "ADG syntaxes does not support multiple scriptlet calls within one single rule" };
	/**
	* Generates a string representation of the AdGuard scriptlet call body.
	*
	* @param node Scriptlet injection rule body
	* @returns String representation of the rule body
	* @throws Error if the scriptlet call has multiple parameters
	*/
	static generate(node) {
		const result = [];
		if (node.children.length > 1) throw new Error(AdgScriptletInjectionBodyGenerator.ERROR_MESSAGES.NO_MULTIPLE_SCRIPTLET_CALLS);
		result.push(ADG_SCRIPTLET_MASK);
		result.push("(");
		if (node.children.length > 0) result.push(ParameterListGenerator.generate(node.children[0]));
		result.push(")");
		return result.join("");
	}
};
//#endregion
export { AdgScriptletInjectionBodyGenerator };
