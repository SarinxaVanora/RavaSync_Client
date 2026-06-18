import { isUndefined } from "../../utils/type-guards.js";
import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/comment/agent-generator.js
/**
* Generator for adblock agent nodes.
* This class is responsible for converting adblock agent nodes into their string representation.
*/
var AgentGenerator = class extends BaseGenerator {
	/**
	* Converts an adblock agent node to a string.
	*
	* @param value Agent node
	* @returns Raw string
	*/
	static generate(value) {
		let result = "";
		result += value.adblock.value;
		if (!isUndefined(value.version)) {
			result += " ";
			result += value.version.value;
		}
		return result;
	}
};
//#endregion
export { AgentGenerator };
