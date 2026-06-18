import { BaseGenerator } from "../base-generator.js";
import { ParameterListGenerator } from "../misc/parameter-list-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/config-comment-generator.js
/**
* Converts inline configuration comment nodes to their string format.
*/
var ConfigCommentGenerator = class extends BaseGenerator {
	/**
	* Converts an inline configuration comment node to a string.
	*
	* @param node Inline configuration comment node
	* @returns Raw string
	*/
	static generate(node) {
		let result = "";
		result += node.marker.value;
		result += " ";
		result += node.command.value;
		if (node.params) {
			result += " ";
			if (node.params.type === "ParameterList") result += ParameterListGenerator.generate(node.params, ",");
			else result += JSON.stringify(node.params.value).slice(1, -1).trim();
		}
		if (node.comment) {
			result += " ";
			result += node.comment.value;
		}
		return result;
	}
};
//#endregion
export { ConfigCommentGenerator };
