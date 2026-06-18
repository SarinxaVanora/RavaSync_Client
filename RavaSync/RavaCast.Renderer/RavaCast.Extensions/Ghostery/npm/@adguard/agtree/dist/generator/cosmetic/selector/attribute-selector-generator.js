import { QuoteType, QuoteUtils } from "../../../utils/quotes.js";
import { BaseGenerator } from "../../base-generator.js";
import { ValueGenerator } from "../../misc/value-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/attribute-selector-generator.js
/**
* Attribute selector generator.
*/
var AttributeSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the attribute selector.
	*
	* @param node Attribute selector node.
	*
	* @returns String representation of the attribute selector.
	*/
	static generate(node) {
		const result = [];
		result.push("[");
		result.push(ValueGenerator.generate(node.name));
		if ("value" in node) {
			result.push(ValueGenerator.generate(node.operator));
			const generatedValue = ValueGenerator.generate(node.value);
			const quotedValue = QuoteUtils.setStringQuoteType(generatedValue, QuoteType.Double);
			result.push(quotedValue);
			if (node.flag) {
				result.push(" ");
				result.push(ValueGenerator.generate(node.flag));
			}
		}
		result.push("]");
		return result.join("");
	}
};
//#endregion
export { AttributeSelectorGenerator };
