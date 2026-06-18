import { BaseGenerator } from "../../base-generator.js";
import { ParameterListGenerator } from "../../misc/parameter-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/scriptlet-body/ubo-scriptlet-injection-body-generator.js
/**
* uBlock scriptlet injection body generator.
*/
var UboScriptletInjectionBodyGenerator = class UboScriptletInjectionBodyGenerator extends BaseGenerator {
	/**
	* Error messages used by the generator.
	*/
	static ERROR_MESSAGES = { NO_MULTIPLE_SCRIPTLET_CALLS: "uBO syntaxes does not support multiple scriptlet calls within one single rule" };
	/**
	* Generates a string representation of the uBlock scriptlet call body.
	*
	* @param node Scriptlet injection rule body
	* @returns String representation of the rule body
	* @throws Error if the scriptlet call has multiple parameters
	*/
	static generate(node) {
		const result = [];
		if (node.children.length > 1) throw new Error(UboScriptletInjectionBodyGenerator.ERROR_MESSAGES.NO_MULTIPLE_SCRIPTLET_CALLS);
		result.push("+js");
		result.push("(");
		if (node.children.length > 0) {
			const [parameterListNode] = node.children;
			result.push(ParameterListGenerator.generate(parameterListNode));
		}
		result.push(")");
		return result.join("");
	}
};
//#endregion
export { UboScriptletInjectionBodyGenerator };
