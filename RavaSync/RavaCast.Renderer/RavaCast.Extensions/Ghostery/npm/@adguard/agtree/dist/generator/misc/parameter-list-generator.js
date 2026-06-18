import { BaseGenerator } from "../base-generator.js";
import { ValueGenerator } from "./value-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/parameter-list-generator.js
/**
* Generator for parameter list nodes.
*/
var ParameterListGenerator = class extends BaseGenerator {
	/**
	* Converts a parameter list AST to a string.
	*
	* @param params Parameter list AST
	* @param separator Separator character (default: comma)
	* @param allowSpace Allow space between parameters (default: true)
	* @returns String representation of the parameter list
	*/
	static generate(params, separator = ",", allowSpace = true) {
		const collection = [];
		let i = 0;
		for (; i < params.children.length; i += 1) {
			const param = params.children[i];
			if (param === null) collection.push("");
			else collection.push(ValueGenerator.generate(param));
		}
		let result = "";
		if (!allowSpace && separator !== " ") result = collection.join(separator);
		else result = collection.join(separator === " " ? separator : `${separator} `);
		return result;
	}
};
//#endregion
export { ParameterListGenerator };
