import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/value-generator.js
/**
* Generator for value nodes.
*/
var ValueGenerator = class extends BaseGenerator {
	/**
	* Converts a value node to a string.
	*
	* @param node Value node.
	* @returns Raw string.
	*/
	static generate(node) {
		return node.value;
	}
};
//#endregion
export { ValueGenerator };
