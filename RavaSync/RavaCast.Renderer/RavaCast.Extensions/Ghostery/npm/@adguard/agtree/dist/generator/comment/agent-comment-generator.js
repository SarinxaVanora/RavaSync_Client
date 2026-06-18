import { BaseGenerator } from "../base-generator.js";
import { AgentGenerator } from "./agent-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/agent-comment-generator.js
/**
* Generator for agent comment rules.
*/
var AgentCommentGenerator = class extends BaseGenerator {
	/**
	* Converts an adblock agent AST to a string.
	*
	* @param ast Agent rule AST
	* @returns Raw string
	*/
	static generate(ast) {
		let result = "[";
		result += ast.children.map(AgentGenerator.generate).join("; ");
		result += "]";
		return result;
	}
};
//#endregion
export { AgentCommentGenerator };
