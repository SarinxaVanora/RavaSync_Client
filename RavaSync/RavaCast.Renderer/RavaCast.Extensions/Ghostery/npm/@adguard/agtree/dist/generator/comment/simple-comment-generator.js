import { BaseGenerator } from "../base-generator.js";
import { ValueGenerator } from "../misc/value-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/simple-comment-generator.js
/**
* Simple comment generator.
*/
var SimpleCommentGenerator = class extends BaseGenerator {
	/**
	* Converts a comment rule node to a string.
	*
	* @param node Comment rule node.
	* @returns Raw string.
	*/
	static generate(node) {
		let result = "";
		result += ValueGenerator.generate(node.marker);
		result += ValueGenerator.generate(node.text);
		return result;
	}
};
//#endregion
export { SimpleCommentGenerator };
