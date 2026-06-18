import { NotImplementedError } from "../errors/not-implemented-error.js";
//#region node_modules/@adguard/agtree/dist/generator/base-generator.js
/**
* @file Base generator class.
*/
/**
* Base class for generators. Each generator should extend this class.
*/
var BaseGenerator = class {
	/**
	* Generates a string from the AST node.
	*
	* @param node AST node to generate a string from.
	*/
	static generate(node) {
		throw new NotImplementedError();
	}
};
//#endregion
export { BaseGenerator };
