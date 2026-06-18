import { CSS_MEDIA_MARKER } from "../../utils/constants.js";
import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/css/adg-css-injection-generator.js
/**
* AdGuard CSS injection generator.
*/
var AdgCssInjectionGenerator = class AdgCssInjectionGenerator extends BaseGenerator {
	static REMOVE_DECLARATION = "remove: true;";
	/**
	* Serializes an AdGuard CSS injection node into a raw string.
	*
	* @param node Node to serialize.
	* @returns Raw string.
	*/
	static generate(node) {
		const result = [];
		if (node.mediaQueryList) result.push(CSS_MEDIA_MARKER, " ", node.mediaQueryList.value, " ", "{", " ");
		result.push(node.selectorList.value, " ", "{", " ");
		if (node.remove) result.push(AdgCssInjectionGenerator.REMOVE_DECLARATION);
		else if (node.declarationList?.value) result.push(node.declarationList.value);
		result.push(" ", "}");
		if (node.mediaQueryList) result.push(" ", "}");
		return result.join("");
	}
};
//#endregion
export { AdgCssInjectionGenerator };
