import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/network/host-rule-generator.js
/**
* Generator for host rule nodes.
*/
var HostRuleGenerator = class extends BaseGenerator {
	/**
	* Converts a host rule node to a raw string.
	*
	* @param node Host rule node.
	* @returns Raw string.
	*/
	static generate(node) {
		const result = [];
		if (node.ip) result.push(node.ip.value);
		if (node.hostnames) {
			result.push(" ");
			result.push(node.hostnames.children.map(({ value }) => value).join(" "));
		}
		if (node.comment) {
			result.push(" ");
			result.push("#");
			result.push(" ");
			result.push(node.comment.value);
		}
		return result.join("");
	}
};
//#endregion
export { HostRuleGenerator };
