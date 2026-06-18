import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/modifier-generator.js
/**
* Generator for modifier nodes.
*/
var ModifierGenerator = class extends BaseGenerator {
	/**
	* Converts a modifier AST node to a string.
	*
	* @param modifier Modifier AST node to convert
	* @returns String representation of the modifier
	*/
	static generate(modifier) {
		let result = "";
		if (modifier.exception) result += "~";
		result += modifier.name.value;
		if (modifier.value !== void 0) {
			result += "=";
			result += modifier.value.value;
		}
		return result;
	}
};
//#endregion
export { ModifierGenerator };
