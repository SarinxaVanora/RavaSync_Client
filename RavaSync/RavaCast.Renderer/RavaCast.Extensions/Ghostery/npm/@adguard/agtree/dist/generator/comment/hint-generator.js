import { BaseGenerator } from "../base-generator.js";
import { ParameterListGenerator } from "../misc/parameter-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/hint-generator.js
/**
* Hint generator.
*/
var HintGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of a hint.
	*
	* @param hint Hint AST node
	* @returns String representation of the hint
	*/
	static generate(hint) {
		let result = "";
		result += hint.name.value;
		if (hint.params && hint.params.children.length > 0) {
			result += "(";
			result += ParameterListGenerator.generate(hint.params, ",");
			result += ")";
		}
		return result;
	}
};
//#endregion
export { HintGenerator };
