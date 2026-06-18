import { BaseGenerator } from "../base-generator.js";
import { ListItemsGenerator } from "./list-items-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/domain-list-generator.js
/**
* Domain list generator.
*/
var DomainListGenerator = class extends BaseGenerator {
	/**
	* Converts a domain list node to a string.
	*
	* @param node Domain list node.
	*
	* @returns Raw string.
	*/
	static generate(node) {
		return ListItemsGenerator.generate(node.children, node.separator);
	}
};
//#endregion
export { DomainListGenerator };
