import { BaseGenerator } from "../base-generator.js";
import { ValueGenerator } from "../misc/value-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/metadata-comment-generator.js
/**
* Metadata comment generator.
*/
var MetadataCommentGenerator = class extends BaseGenerator {
	/**
	* Converts a metadata comment rule node to a string.
	*
	* @param node Metadata comment rule node.
	* @returns Raw string.
	*/
	static generate(node) {
		let result = "";
		result += ValueGenerator.generate(node.marker);
		result += " ";
		result += ValueGenerator.generate(node.header);
		result += ":";
		result += " ";
		result += ValueGenerator.generate(node.value);
		return result;
	}
};
//#endregion
export { MetadataCommentGenerator };
