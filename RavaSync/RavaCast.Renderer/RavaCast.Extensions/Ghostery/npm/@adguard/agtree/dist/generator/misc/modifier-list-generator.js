import { BaseGenerator } from "../base-generator.js";
import { ModifierGenerator } from "./modifier-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/modifier-list-generator.js
/**
* Generator for modifier list nodes.
*/
var ModifierListGenerator = class extends BaseGenerator {
	/**
	* Converts a modifier list AST to a string.
	*
	* @param ast Modifier list AST
	* @returns Raw string
	*/
	static generate(ast) {
		return ast.children.map(ModifierGenerator.generate).join(",");
	}
};
//#endregion
export { ModifierListGenerator };
