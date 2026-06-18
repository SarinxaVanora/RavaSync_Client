import "../../utils/constants.js";
import { BaseGenerator } from "../base-generator.js";
import { ValueGenerator } from "../misc/value-generator.js";
import { ParameterListGenerator } from "../misc/parameter-list-generator.js";
import { LogicalExpressionGenerator } from "../misc/logical-expression-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/pre-processor-comment-generator.js
/**
* Pre-processor comment generator.
*/
var PreProcessorCommentGenerator = class extends BaseGenerator {
	/**
	* Converts a pre-processor comment node to a string.
	*
	* @param node Pre-processor comment node
	* @returns Raw string
	*/
	static generate(node) {
		let result = "";
		result += "!#";
		result += node.name.value;
		if (node.params) {
			let allowSpaceBetweenParams = true;
			if (node.name.value === "safari_cb_affinity") allowSpaceBetweenParams = false;
			if (node.name.value !== "safari_cb_affinity") result += " ";
			if (node.params.type === "Value") result += ValueGenerator.generate(node.params);
			else if (node.params.type === "ParameterList") {
				result += "(";
				result += ParameterListGenerator.generate(node.params, ",", allowSpaceBetweenParams);
				result += ")";
			} else result += LogicalExpressionGenerator.generate(node.params);
		}
		return result;
	}
};
//#endregion
export { PreProcessorCommentGenerator };
