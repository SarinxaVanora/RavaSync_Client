import { BaseGenerator } from "../base-generator.js";
import { HintGenerator } from "./hint-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/hint-comment-generator.js
/**
* Hint comment generator.
*/
var HintCommentGenerator = class extends BaseGenerator {
	/**
	* Converts a hint rule node to a raw string.
	*
	* @param node Hint rule node
	* @returns Raw string
	*/
	static generate(node) {
		let result = "!+ ";
		result += node.children.map(HintGenerator.generate).join(" ");
		return result;
	}
};
//#endregion
export { HintCommentGenerator };
